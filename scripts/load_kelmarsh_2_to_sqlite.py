"""
Load all Turbine_Data_Kelmarsh_2_*.csv files into a SQLite database.

This script expects Kelmarsh CSV files that include a leading comment block followed by
a header line that starts with '# Date and time,...' (exact header format provided by the user).

Behavior (safe defaults):
- Finds files matching the glob pattern (default: Turbine_Data_Kelmarsh_2_*.csv) under the
  input directory (default: data/kelmarsh_data).
- Detects the header line (the line that contains the column names, typically starting with
  '# Date and time'). The header is parsed with Python's csv module to respect quoted fields.
- Reads the CSV data in chunks using pandas, inferring column types from the first chunk.
- Creates a SQLite table whose columns match the header names. Numeric columns map to
  INTEGER/REAL when possible; otherwise TEXT.
- Attempts to parse the 'Date and time' column (if present) and stores it as ISO-formatted UTC
  text (YYYY-MM-DDTHH:MM:SS+0000). This column is used as the default dedupe key.
- Creates a UNIQUE index on the selected dedupe columns and inserts rows with
  INSERT OR IGNORE to avoid duplicates.

Important: This file is a script to be run locally. Per instructions, the assistant will not run it here.

Usage (project root):
python scripts\load_kelmarsh_2_to_sqlite.py \
    --input-dir data/kelmarsh_data \
    --pattern "Turbine_Data_Kelmarsh_2_*.csv" \
    --db-path data/sqlitedbs/kelmarsh_2_data.db \
    --table kelmarsh_2

"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import sqlite3
from typing import List, Optional, Tuple

import pandas as pd


def find_header_line(fpath: str, encoding: str = "utf-8") -> Tuple[Optional[int], Optional[str]]:
    """Return (header_idx, header_line) where header_idx is 0-based index of the header line.
    We prefer the line that contains 'Date and time' and starts with '#'. As a fallback
    we return the first non-comment line containing commas.
    """
    with open(fpath, "r", encoding=encoding, errors="replace") as fh:
        for i, line in enumerate(fh):
            if line.lstrip().startswith("#") and "Date and time" in line:
                return i, line.lstrip("#").strip()
        fh.seek(0)
        for i, line in enumerate(fh):
            if "," in line and not line.lstrip().startswith("#"):
                return i, line.strip()
    return None, None


def parse_header_line(header_line: str) -> List[str]:
    """Parse a CSV header line into column names using csv.reader to handle quotes."""
    # csv.reader expects an iterable of lines
    reader = csv.reader([header_line])
    cols = next(reader)
    # strip whitespace and keep units/characters (° and °C preserved)
    cols = [c.strip() for c in cols]
    return cols


def make_unique_column_names(cols: List[str]) -> List[str]:
    """Return a new list of column names where duplicates are made unique by appending a suffix.
    Keeps the original base names readable but ensures no exact duplicates.
    Example: ['A','B','A'] -> ['A','B','A_2']
    """
    seen = {}
    out = []
    for c in cols:
        base = c
        if base in seen:
            seen[base] += 1
            new = f"{base}_{seen[base]}"
        else:
            seen[base] = 1
            new = base
        out.append(new)
    return out


def map_sql_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series.dropna()):
        return "INTEGER"
    if pd.api.types.is_float_dtype(series.dropna()):
        return "REAL"
    # treat datetimes and others as TEXT
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TEXT"
    return "TEXT"


def infer_schema_from_df(df: pd.DataFrame) -> dict:
    schema = {}
    for col in df.columns:
        schema[col] = map_sql_type(df[col])
    return schema


def create_table(conn: sqlite3.Connection, table: str, schema: dict, if_exists: str = "append") -> None:
    cur = conn.cursor()
    if if_exists == "replace":
        cur.execute(f"DROP TABLE IF EXISTS \"{table}\"")
        conn.commit()
    cols_defs = ", ".join([f'\"{c}\" {schema[c]}' for c in schema])
    sql = f"CREATE TABLE IF NOT EXISTS \"{table}\" ({cols_defs})"
    cur.execute(sql)
    conn.commit()


def create_unique_index(conn: sqlite3.Connection, table: str, cols: List[str]) -> None:
    if not cols:
        return
    index_name = f"ux_{table}_" + "_".join([c.replace(" ", "_") for c in cols])
    cols_sql = ", ".join([f'\"{c}\"' for c in cols])
    sql = f"CREATE UNIQUE INDEX IF NOT EXISTS \"{index_name}\" ON \"{table}\" ({cols_sql})"
    conn.execute(sql)
    conn.commit()


def rows_from_chunk(df: pd.DataFrame, columns: List[str]) -> List[tuple]:
    # convert NaN to None and datetimes to ISO strings
    out = []
    for _, r in df[columns].iterrows():
        row = []
        for v in r:
            if pd.isna(v):
                row.append(None)
            elif hasattr(v, "isoformat") and not isinstance(v, str):
                try:
                    row.append(v.isoformat())
                except Exception:
                    row.append(str(v))
            else:
                row.append(v)
        out.append(tuple(row))
    return out


def default_timestamp_candidates() -> List[str]:
    return ["Date and time", "DateTime", "Date", "timestamp", "Timestamp", "time", "Time"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Kelmarsh turbine 2 CSVs into sqlite")
    parser.add_argument("--input-dir", default="data/kelmarsh_data", help="Directory with CSV files")
    parser.add_argument("--pattern", default="Turbine_Data_Kelmarsh_2_*.csv", help="Glob pattern to match files")
    parser.add_argument("--db-path", default="data/sqlitedbs/kelmarsh_2_data.db", help="Path to output sqlite DB")
    parser.add_argument("--table", default="kelmarsh_2", help="Target table name")
    parser.add_argument("--batch-size", type=int, default=10000, help="Rows per chunk to process")
    parser.add_argument("--encoding", default="utf-8", help="CSV file encoding")
    parser.add_argument("--if-exists", choices=["append", "replace"], default="append", help="Behavior if table exists")
    parser.add_argument("--dedupe-on", default=None, help="Comma-separated columns to dedupe on (defaults to timestamp candidate)")
    args = parser.parse_args()

    input_glob = os.path.join(args.input_dir, args.pattern)
    files = sorted(glob.glob(input_glob))
    if not files:
        print(f"No files matching: {input_glob}")
        return

    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)
    conn = sqlite3.connect(args.db_path)

    table_schema = None
    columns_order: Optional[List[str]] = None
    dedupe_cols: List[str] = []

    for fpath in files:
        print(f"Processing: {fpath}")
        header_idx, header_line = find_header_line(fpath, encoding=args.encoding)
        if header_line is None:
            print(f"  Skipping (no header found): {fpath}")
            continue
        col_names = parse_header_line(header_line)
        # ensure column names are unique (pandas/read_csv with 'names' requires unique names)
        unique_col_names = make_unique_column_names(col_names)

        # read file in chunks using pandas; pass unique names to avoid duplicate-name errors
        reader = pd.read_csv(fpath, names=unique_col_names, header=None, skiprows=header_idx + 1, chunksize=args.batch_size, encoding=args.encoding)

        for chunk in reader:
            # on first chunk, create table if needed
            if columns_order is None:
                columns_order = list(chunk.columns)
                # try to detect timestamp column
                ts_col = None
                if args.dedupe_on:
                    dedupe_cols = [c.strip() for c in args.dedupe_on.split(",") if c.strip()]
                else:
                    # detect timestamp candidate by matching original header names
                    for cand in default_timestamp_candidates():
                        # find index in original col_names, then map to unique name
                        if cand in col_names:
                            idx = col_names.index(cand)
                            ts_col = unique_col_names[idx]
                            break
                    if ts_col:
                        dedupe_cols = [ts_col]

                # infer schema
                table_schema = infer_schema_from_df(chunk)
                create_table(conn, args.table, table_schema, if_exists=args.if_exists)
                if dedupe_cols:
                    create_unique_index(conn, args.table, dedupe_cols)

            # parse timestamp column to ISO UTC if present
            if dedupe_cols:
                # parse the dedupe/timestamp column(s) if present
                for cand in dedupe_cols:
                    if cand in chunk.columns:
                        try:
                            chunk[cand] = pd.to_datetime(chunk[cand], errors="coerce", utc=True)
                            chunk[cand] = chunk[cand].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                        except Exception:
                            pass

            # ensure all expected columns present
            if columns_order:
                for c in columns_order:
                    if c not in chunk.columns:
                        chunk[c] = None

            rows = rows_from_chunk(chunk, columns_order or list(chunk.columns))
            if not rows:
                continue

            placeholders = ",".join(["?" for _ in (columns_order or list(chunk.columns))])
            cols_sql = ",".join([f'\"{c}\"' for c in (columns_order or list(chunk.columns))])
            insert_sql = f'INSERT OR IGNORE INTO \"{args.table}\" ({cols_sql}) VALUES ({placeholders})'
            cur = conn.cursor()
            cur.executemany(insert_sql, rows)
            conn.commit()

    conn.close()
    print(f"Finished. Database written to: {args.db_path}")


if __name__ == "__main__":
    main()

