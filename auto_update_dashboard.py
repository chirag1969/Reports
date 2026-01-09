#!/usr/bin/env python3
"""Continuously publish the Products Search Term worksheet for GitHub Pages."""
from __future__ import annotations

import json
import logging
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


# =====================
# Configurable settings
# =====================
REPO_ROOT = Path(__file__).resolve().parent
EXCEL_PATH = REPO_ROOT / "Products Search Term.xlsx"
SHEET_NAME = "Sheet 1"
OUTPUT_PATH = REPO_ROOT / "preprocessed/Products Search Term.json"
HEADER_ROW_INDEX = 1
CONFIG_PATH = REPO_ROOT / "tools/local_pipeline_config.json"
DEBOUNCE_SECONDS = 2.5
POLL_INTERVAL_SECONDS = 5.0
GIT_COMMIT_MESSAGE = "Update Products Search Term data"
GIT_REMOTE_NAME = "origin"
GIT_BRANCH_NAME = None


@dataclass(frozen=True)
class WorkbookConfig:
    excel_path: Path
    sheet_name: str
    output_path: Path
    header_row_index: int


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
    def __init__(self, watch_paths: set[Path], runner: DebouncedRunner) -> None:
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


def load_config_from_file(config_path: Path) -> Optional[WorkbookConfig]:
    if not config_path.exists():
        return None
    try:
        with config_path.open("r", encoding="utf-8") as fp:
            payload = json.load(fp)
    except json.JSONDecodeError:
        logging.warning("Config file is not valid JSON: %s", config_path)
        return None
    workbooks = payload.get("workbooks", [])
    if not isinstance(workbooks, list):
        logging.warning("Config file missing 'workbooks' list: %s", config_path)
        return None
    for entry in workbooks:
        sheet_name = str(entry.get("sheet_name", "")).strip()
        if sheet_name.lower() != SHEET_NAME.lower():
            continue
        excel_path_raw = str(entry.get("excel_path", "")).strip()
        output_path_raw = str(entry.get("output_path", "")).strip()
        header_row_index = int(entry.get("header_row_index", HEADER_ROW_INDEX))
        if not excel_path_raw or not output_path_raw:
            continue
        excel_path = Path(excel_path_raw).expanduser().resolve()
        output_path = Path(output_path_raw)
        if not output_path.is_absolute():
            output_path = REPO_ROOT / output_path
        return WorkbookConfig(
            excel_path=excel_path,
            sheet_name=SHEET_NAME,
            output_path=output_path,
            header_row_index=header_row_index,
        )
    return None


def resolve_workbook_config() -> WorkbookConfig:
    config = load_config_from_file(CONFIG_PATH)
    if config is not None:
        return config
    return WorkbookConfig(
        excel_path=EXCEL_PATH,
        sheet_name=SHEET_NAME,
        output_path=OUTPUT_PATH,
        header_row_index=HEADER_ROW_INDEX,
    )


def normalize_cell(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is not None:
            value = value.tz_convert(timezone.utc)
        if value.hour == 0 and value.minute == 0 and value.second == 0:
            return value.date().isoformat()
        return value.isoformat()
    if isinstance(value, str):
        return value.strip()
    return value


def trim_trailing(values: list[object]) -> list[object]:
    idx = len(values)
    while idx and values[idx - 1] in (None, ""):
        idx -= 1
    return values[:idx]


def iter_sheet_rows(df: pd.DataFrame, header_row_index: int) -> Iterable[list[object]]:
    rows = df.values.tolist()
    pre_rows: list[list[object]] = []
    target_width: Optional[int] = None
    yielded_header = False
    for idx, row in enumerate(rows):
        cleaned = [normalize_cell(value) for value in row]
        cleaned = trim_trailing(cleaned)
        if idx < header_row_index:
            pre_rows.append(cleaned)
            continue
        if idx == header_row_index:
            if not cleaned:
                raise ValueError("Header row appears to be empty")
            target_width = len(cleaned)
            for pre in pre_rows:
                yield normalize_row(pre, target_width)
            yield normalize_row(cleaned, target_width)
            yielded_header = True
            continue
        if target_width is None:
            raise ValueError("Header row not found while iterating the worksheet")
        yield normalize_row(cleaned, target_width)
    if not yielded_header:
        raise ValueError("No usable sheet found (header row missing)")


def normalize_row(values: list[object], width: int) -> list[object]:
    trimmed = trim_trailing(values)
    trimmed = trimmed[:width]
    if len(trimmed) < width:
        trimmed.extend([None] * (width - len(trimmed)))
    return trimmed


def read_worksheet(config: WorkbookConfig) -> list[list[object]]:
    if not config.excel_path.exists():
        raise FileNotFoundError(f"Excel file not found: {config.excel_path}")
    try:
        df = pd.read_excel(
            config.excel_path,
            sheet_name=config.sheet_name,
            header=None,
            engine="openpyxl",
            dtype=object,
        )
    except ValueError as exc:
        raise ValueError(
            f"Sheet '{config.sheet_name}' not found in {config.excel_path.name}."
        ) from exc

    if df.empty:
        raise ValueError("Sheet is empty")

    rows = list(iter_sheet_rows(df, config.header_row_index))
    if not rows:
        raise ValueError("Sheet is empty")
    return rows


def write_json_output(config: WorkbookConfig, rows: Iterable[list[object]]) -> None:
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "source": config.excel_path.name,
        "sheet_name": config.sheet_name,
        "header_row_index": config.header_row_index,
        "generated_at": generated_at,
        "sheet_data": list(rows),
    }
    with config.output_path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    logging.info("Wrote %s", config.output_path)


def git_publish(output_path: Path) -> None:
    rel_path = str(output_path)
    subprocess.run(["git", "add", "--", rel_path], check=True)
    status = subprocess.check_output(
        ["git", "status", "--porcelain", "--", rel_path],
        text=True,
    ).strip()
    if not status:
        logging.info("No changes detected; skipping commit.")
        return
    subprocess.run(["git", "commit", "-m", GIT_COMMIT_MESSAGE], check=True)
    push_cmd = ["git", "push", GIT_REMOTE_NAME]
    if GIT_BRANCH_NAME:
        push_cmd.append(GIT_BRANCH_NAME)
    subprocess.run(push_cmd, check=True)


def refresh_dashboard() -> None:
    config = resolve_workbook_config()
    try:
        rows = read_worksheet(config)
    except FileNotFoundError as exc:
        logging.warning("%s", exc)
        return
    except ValueError as exc:
        logging.warning("%s", exc)
        return

    if not rows:
        logging.warning("Sheet is empty")
        return

    write_json_output(config, rows)
    try:
        git_publish(config.output_path)
    except subprocess.CalledProcessError as exc:
        logging.error("Git publish failed: %s", exc)


def watch_with_watchdog(config: WorkbookConfig) -> Observer:
    runner = DebouncedRunner(DEBOUNCE_SECONDS, refresh_dashboard)
    handler = ExcelChangeHandler({config.excel_path, CONFIG_PATH}, runner)
    observer = Observer()
    watch_dirs = {config.excel_path.parent, CONFIG_PATH.parent}
    for watch_dir in watch_dirs:
        observer.schedule(handler, str(watch_dir), recursive=False)
    observer.start()
    return observer


def poll_for_changes(config: WorkbookConfig) -> None:
    watch_paths = {config.excel_path, CONFIG_PATH}
    mtimes: dict[Path, float] = {}
    for path in watch_paths:
        if path.exists():
            mtimes[path] = path.stat().st_mtime
    while True:
        time.sleep(POLL_INTERVAL_SECONDS)
        updated = False
        for path in watch_paths:
            if not path.exists():
                continue
            mtime = path.stat().st_mtime
            if mtimes.get(path) != mtime:
                mtimes[path] = mtime
                updated = True
        if updated:
            refresh_dashboard()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    config = resolve_workbook_config()
    logging.info("Watching %s", config.excel_path)
    refresh_dashboard()

    try:
        observer = watch_with_watchdog(config)
    except Exception as exc:
        logging.warning("watchdog unavailable (%s); falling back to polling", exc)
        poll_for_changes(config)
        return

    last_config_mtime = CONFIG_PATH.stat().st_mtime if CONFIG_PATH.exists() else 0.0
    try:
        while True:
            time.sleep(1)
            if CONFIG_PATH.exists():
                current_mtime = CONFIG_PATH.stat().st_mtime
                if current_mtime != last_config_mtime:
                    last_config_mtime = current_mtime
                    logging.info("Config updated; reloading watch paths.")
                    observer.stop()
                    observer.join()
                    config = resolve_workbook_config()
                    observer = watch_with_watchdog(config)
    except KeyboardInterrupt:
        logging.info("Stopping watcher.")
    finally:
        observer.stop()
        observer.join()


if __name__ == "__main__":
    sys.exit(main())
