#!/usr/bin/env python3
"""Preprocess a large Excel workbook into partitioned JSON slices for the dashboard.

The dashboard is hosted on GitHub Pages, therefore we cannot rely on any
server-side querying.  This script prepares the workbook so that the frontend
can selectively download small JSON chunks based on the active filters instead
of transferring the whole dataset on the first paint.

Usage example::

    $ python tools/preprocess_dashboard.py source.xlsx --out data \
          --filters date store category targetingType asin \
          --partition date:month store

This writes ``data/index.json`` with metadata plus one JSON file per partition.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Mapping, MutableMapping, Sequence

import numpy as np
import pandas as pd

ISO_DATE_FORMATS = ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workbook", type=Path, help="Path to the Excel workbook")
    parser.add_argument(
        "--sheet",
        help="Optional sheet name. When omitted, the first sheet is used.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data"),
        help="Output directory for generated files (default: ./data)",
    )
    parser.add_argument(
        "--filters",
        nargs="*",
        default=["date", "category", "store", "targetingType", "asin"],
        help="Columns that can be used as filters on the dashboard.",
    )
    parser.add_argument(
        "--partition",
        nargs="*",
        default=["date:month"],
        help=(
            "Columns used to partition the dataset. You can suffix a column with "
            "':month' or ':year' to bucket by that period (e.g. date:month)."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("json", "csv.gz"),
        default="json",
        help="Format for partition slices (default: json)",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=None,
        help="Optional indentation level for JSON output.",
    )
    return parser.parse_args()


def ensure_output_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_workbook(path: Path, sheet: str | None) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
    df = df.dropna(axis=0, how="all")
    df = df.rename(columns=lambda c: str(c).strip())
    df = df.replace({"": pd.NA})
    return df


def coerce_dates(df: pd.DataFrame, columns: Iterable[str]) -> None:
    for column in columns:
        if column not in df.columns:
            continue
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = df[column].dt.date
            continue
        for fmt in ISO_DATE_FORMATS:
            try:
                df[column] = pd.to_datetime(df[column], format=fmt, errors="raise").dt.date
            except (ValueError, TypeError):
                continue
            else:
                break
        df[column] = pd.to_datetime(df[column], errors="coerce").dt.date


def fill_missing_numeric(df: pd.DataFrame) -> None:
    numeric_columns = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if numeric_columns:
        df[numeric_columns] = df[numeric_columns].fillna(0)


def to_json_primitive(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (np.floating, np.integer)):
        cast_value = value.item()
        if isinstance(cast_value, (np.floating, float)):
            return float(cast_value)
        if isinstance(cast_value, (np.integer, int)):
            return int(cast_value)
        return cast_value
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def slugify(text: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in text).strip("-")


def hash_key(parts: Sequence[str]) -> str:
    data = "|".join(part or "" for part in parts)
    return hashlib.sha1(data.encode("utf-8")).hexdigest()[:12]


def expand_partition_value(column: str, value: object, rule: str | None) -> object:
    if rule is None:
        return value
    if rule == "month":
        if isinstance(value, (dt.date, dt.datetime)):
            return dt.date(value.year, value.month, 1)
        value = pd.to_datetime(value, errors="coerce")
        if pd.isna(value):
            return None
        value = value.to_pydatetime()
        return dt.date(value.year, value.month, 1)
    if rule == "year":
        if isinstance(value, (dt.date, dt.datetime)):
            return dt.date(value.year, 1, 1)
        value = pd.to_datetime(value, errors="coerce")
        if pd.isna(value):
            return None
        value = value.to_pydatetime()
        return dt.date(value.year, 1, 1)
    raise ValueError(f"Unknown partition rule '{rule}' for column '{column}'")


@dataclass
class PartitionDescriptor:
    key: str
    filters: Mapping[str, object]
    row_count: int
    path: Path


def iter_partitions(
    df: pd.DataFrame, partition_spec: Sequence[str], base_path: Path, fmt: str
) -> Iterator[PartitionDescriptor]:
    if not partition_spec:
        yield write_partition_slice(df, base_path / "all", fmt, {})
        return

    processed_spec: List[tuple[str, str | None]] = []
    for entry in partition_spec:
        if ":" in entry:
            column, rule = entry.split(":", 1)
        else:
            column, rule = entry, None
        processed_spec.append((column, rule))

    def derive_bucket(row: Mapping[str, object]) -> tuple:
        bucket: List[object] = []
        for column, rule in processed_spec:
            value = row.get(column)
            bucket.append(expand_partition_value(column, value, rule))
        return tuple(bucket)

    bucketed: MutableMapping[tuple, List[int]] = {}
    for idx, row in df.iterrows():
        key = derive_bucket(row)
        bucketed.setdefault(key, []).append(idx)

    for bucket_values, indices in bucketed.items():
        key = hash_key([str(to_json_primitive(v) or "") for v in bucket_values])
        filters = {
            column: to_json_primitive(value) for (column, _), value in zip(processed_spec, bucket_values)
        }
        slice_df = df.loc[indices]
        directory = base_path / key
        yield write_partition_slice(slice_df, directory, fmt, filters)


def write_partition_slice(
    df: pd.DataFrame, directory: Path, fmt: str, filters: Mapping[str, object]
) -> PartitionDescriptor:
    ensure_output_directory(directory)
    key = directory.name
    if fmt == "json":
        payload = {
            "columns": list(df.columns),
            "data": [
                [to_json_primitive(value) for value in row]
                for row in df.itertuples(index=False, name=None)
            ],
        }
        output_path = directory / "data.json"
        with output_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False)
    else:
        output_path = directory / "data.csv.gz"
        df.to_csv(output_path, index=False, compression="gzip")
    return PartitionDescriptor(key=key, filters=filters, row_count=len(df), path=output_path)


def build_filter_index(df: pd.DataFrame, filter_columns: Sequence[str]) -> Dict[str, List[str]]:
    index: Dict[str, List[str]] = {}
    for column in filter_columns:
        if column not in df.columns:
            continue
        values: List[str] = []
        for raw in df[column].dropna():
            converted = to_json_primitive(raw)
            if converted is None:
                continue
            text = str(converted).strip()
            if not text:
                continue
            values.append(text)
        index[column] = sorted(dict.fromkeys(values))
    return index


def main() -> None:
    args = parse_args()
    workbook = args.workbook.resolve()
    if not workbook.exists():
        raise SystemExit(f"Workbook not found: {workbook}")

    ensure_output_directory(args.out)

    df = read_workbook(workbook, args.sheet)
    coerce_dates(df, [col for col in df.columns if "date" in col.lower()])
    fill_missing_numeric(df)

    filter_index = build_filter_index(df, args.filters)

    partitions: List[PartitionDescriptor] = list(
        iter_partitions(df, args.partition, args.out, args.format)
    )

    metadata = {
        "version": 1,
        "source": workbook.name,
        "generatedAt": dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat(),
        "rowCount": int(df.shape[0]),
        "columns": list(df.columns),
        "filters": filter_index,
        "partitions": [
            {
                "key": partition.key,
                "filters": partition.filters,
                "rowCount": partition.row_count,
                "path": partition.path.relative_to(args.out).as_posix(),
            }
            for partition in partitions
        ],
        "format": args.format,
    }

    metadata_path = args.out / "index.json"
    with metadata_path.open("w", encoding="utf-8") as fp:
        json.dump(metadata, fp, ensure_ascii=False, indent=args.indent)
        fp.write("\n")

    print(f"Wrote {metadata_path} and {len(partitions)} partition(s)")


if __name__ == "__main__":
    main()
