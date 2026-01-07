#!/usr/bin/env python3
"""Watch local Excel files and auto-publish dashboard data to GitHub Pages.

This script monitors the Excel files listed in tools/local_pipeline_config.json,
regenerates the JSON/CSV outputs expected by the dashboard, and automatically
commits + pushes updates to the repository whenever the Excel files change.
"""
from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Iterable, List, Optional, Set


try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
except ImportError:  # pragma: no cover - fallback when watchdog isn't installed
    FileSystemEventHandler = object  # type: ignore[assignment]
    Observer = None  # type: ignore[assignment]

REPO_ROOT = Path(__file__).resolve().parent
TOOLS_DIR = REPO_ROOT / "tools"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools import local_pipeline  # noqa: E402


DEFAULT_CONFIG = TOOLS_DIR / "local_pipeline_config.json"
DEFAULT_DEBOUNCE_SECONDS = 3.0
DEFAULT_POLL_SECONDS = 10.0


class DebouncedRunner:
    def __init__(self, delay: float, callback) -> None:
        self._delay = delay
        self._callback = callback
        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()

    def trigger(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(self._delay, self._run)
            self._timer.daemon = True
            self._timer.start()

    def _run(self) -> None:
        with self._lock:
            self._timer = None
        self._callback()


class ExcelChangeHandler(FileSystemEventHandler):
    def __init__(self, watch_paths: Set[Path], runner: DebouncedRunner) -> None:
        self.watch_paths = watch_paths
        self.runner = runner

    def on_modified(self, event) -> None:
        self._handle_event(event)

    def on_created(self, event) -> None:
        self._handle_event(event)

    def _handle_event(self, event) -> None:
        if getattr(event, "is_directory", False):
            return
        event_path = Path(getattr(event, "src_path", ""))
        if event_path in self.watch_paths:
            logging.info("Detected update for %s", event_path)
            self.runner.trigger()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to the JSON configuration file.",
    )
    parser.add_argument(
        "--debounce",
        type=float,
        default=DEFAULT_DEBOUNCE_SECONDS,
        help="Seconds to debounce rapid file changes.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=DEFAULT_POLL_SECONDS,
        help="Polling interval when watchdog is unavailable.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one refresh immediately and exit.",
    )
    return parser.parse_args()


def load_watch_paths(config_path: Path) -> List[Path]:
    config = local_pipeline.load_config(config_path)
    paths: List[Path] = []
    for entry in config.get("workbooks", []):
        excel_path = str(entry.get("excel_path", "")).strip()
        if excel_path:
            paths.append(Path(excel_path).expanduser().resolve())
    paths.append(config_path)
    return paths


def run_pipeline(config_path: Path) -> None:
    logging.info("Running data pipeline for %s", config_path)
    config = local_pipeline.load_config(config_path)
    outputs = local_pipeline.process_workbooks(config, only_names=[])
    commit_message = config.get("commit_message", "Update preprocessed dashboard data")
    local_pipeline.git_publish(outputs, str(commit_message), push=True)


def poll_for_changes(config_path: Path, watch_paths: Iterable[Path], interval: float) -> None:
    mtimes: dict[Path, float] = {}
    for path in watch_paths:
        if path.exists():
            mtimes[path] = path.stat().st_mtime
    while True:
        time.sleep(interval)
        updated = False
        for path in watch_paths:
            if not path.exists():
                continue
            mtime = path.stat().st_mtime
            if mtimes.get(path) != mtime:
                mtimes[path] = mtime
                updated = True
        if updated:
            run_pipeline(config_path)


def watch_with_watchdog(config_path: Path, watch_paths: List[Path], debounce: float) -> Observer:
    runner = DebouncedRunner(debounce, lambda: run_pipeline(config_path))
    handler = ExcelChangeHandler(set(watch_paths), runner)
    observer: Observer = Observer()
    watched_dirs = {path.parent for path in watch_paths}
    for watch_dir in watched_dirs:
        observer.schedule(handler, str(watch_dir), recursive=False)
    observer.start()
    return observer


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    config_path = args.config.expanduser().resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    watch_paths = load_watch_paths(config_path)
    if not watch_paths:
        raise ValueError("No Excel paths found in config file")

    if args.once:
        run_pipeline(config_path)
        return

    if Observer is None:
        logging.warning("watchdog is not installed; falling back to polling.")
        poll_for_changes(config_path, watch_paths, args.poll_seconds)
        return

    observer = watch_with_watchdog(config_path, watch_paths, args.debounce)
    logging.info("Watching %s for changes", ", ".join(str(p) for p in watch_paths))
    last_config_mtime = config_path.stat().st_mtime
    try:
        while True:
            time.sleep(1)
            current_mtime = config_path.stat().st_mtime
            if current_mtime != last_config_mtime:
                last_config_mtime = current_mtime
                updated_paths = load_watch_paths(config_path)
                if set(updated_paths) != set(watch_paths):
                    logging.info("Config updated; refreshing watch paths.")
                    observer.stop()
                    observer.join()
                    watch_paths = updated_paths
                    observer = watch_with_watchdog(config_path, watch_paths, args.debounce)
    except KeyboardInterrupt:
        logging.info("Stopping watcher.")
    finally:
        observer.stop()
        observer.join()


if __name__ == "__main__":
    main()
