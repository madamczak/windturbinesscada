"""
Load all Turbine_Data_Kelmarsh_2_*.csv files into a SQLite database.

Usage examples (run from project root):
python scripts\load_kelmarsh_turbine2.py \
    --input-dir data/kelmarsh_data \
    --pattern "Turbine_Data_Kelmarsh_2_*.csv" \
    --db-path data/sqlitedbs/kelmarsh_2.db \
    --table turbine_data_2 \
    --batch-size 10000

The script reads CSVs in chunks, normalizes a timestamp column (auto-detected by default),
creates a table (inferring column types) and a unique index on dedupe columns to avoid
inserting duplicates. The default dedupe column is the detected timestamp column.

Note: This script requires pandas. See requirements.txt.
"""

import argparse
import glob
import os
import sqlite3
import pandas as pd
import math


def detect_timestamp_col(df: pd.DataFrame):
    candidates = [
        "timestamp",
        "Timestamp",
        "time",
        "Time",
        "datetime",
        "DateTime",
        "date",
        "Date",
        "ts",
        "Date and time",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    # try parsing object columns
    for c in df.columns:
        if pd.api.types.is_object_dtype(df[c]) or pd.api.types.is_string_dtype(df[c]):
            try:
                pd.to_datetime(df[c].iloc[:10], errors="raise")
                return c
            except Exception:
                continue
    return None


def map_sql_types_from_series(s: pd.Series):
    # conservative mapping
    if pd.api.types.is_integer_dtype(s):
        return "INTEGER"
    if pd.api.types.is_float_dtype(s):
        return "REAL"
    if pd.api.types.is_bool_dtype(s):
        return "INTEGER"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "TEXT"
    return "TEXT"


def infer_schema_from_df(df: pd.DataFrame):
    dtypes = {}
    for col in df.columns:
        dtypes[col] = map_sql_types_from_series(df[col])
    return dtypes


def create_table_if_needed(conn: sqlite3.Connection, table_name: str, schema: dict, if_exists: str = "append"):
    cur = conn.cursor()
    if if_exists == "replace":
        cur.execute(f"DROP TABLE IF EXISTS \"{table_name}\"")
        conn.commit()
    # ensure table exists
    cols_defs = ", ".join([f'\"{c}\" {schema[c]}' for c in schema])
    sql = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({cols_defs})"
    cur.execute(sql)
    conn.commit()


def create_unique_index(conn: sqlite3.Connection, table_name: str, index_cols: list):
    if not index_cols:
        return
    index_name = f"ux_{table_name}_" + "_".join(index_cols)
    cols_sql = ", ".join([f'\"{c}\"' for c in index_cols])
    sql = f"CREATE UNIQUE INDEX IF NOT EXISTS \"{index_name}\" ON \"{table_name}\" ({cols_sql})"
    conn.execute(sql)
    conn.commit()


def rows_from_df(df: pd.DataFrame, columns: list):
    # convert NaN to None and datetimes to isostring
    rows = []
    for _, r in df[columns].iterrows():
        row = []
        for v in r:
            if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
                row.append(None)
            elif hasattr(v, "isoformat") and not isinstance(v, str):
                # datetime-like
                try:
                    row.append(v.isoformat())
                except Exception:
                    row.append(str(v))
            else:
                row.append(v)
        rows.append(tuple(row))
    return rows


def _find_header_line_index(fpath: str, encoding: str = "utf-8"):
    """Return (header_index, header_line) where header_index is 0-based line index of the header line
    The Kelmarsh CSVs include a comment block with lines starting '#' and a header line that also
    starts with '# Date and time,...' — we detect that and return its index and the cleaned header line.
    """
    with open(fpath, "r", encoding=encoding, errors="replace") as fh:
        header_idx = None
        header_line = None
        for i, line in enumerate(fh):
            # common header marker in these files
            if line.lstrip().startswith("#") and "Date and time" in line:
                header_idx = i
                header_line = line
                break
        if header_line is None:
            # fallback: first line that contains commas and is not a pure comment block
            fh.seek(0)
            for i, line in enumerate(fh):
                if "," in line and not line.lstrip().startswith("#"):
                    header_idx = i
                    header_line = line
                    break
    return header_idx, header_line


def main():
    parser = argparse.ArgumentParser(description="Load Kelmarsh turbine 2 CSVs into sqlite")
    parser.add_argument("--input-dir", default="data/kelmarsh_data", help="Directory with CSV files")
    parser.add_argument("--pattern", default="Turbine_Data_Kelmarsh_2_*.csv", help="Glob pattern to match files")
    parser.add_argument("--db-path", default="data/sqlitedbs/kelmarsh_2.db", help="Output sqlite database path")
    parser.add_argument("--table", default="turbine_2", help="Table name to write")
    parser.add_argument("--timestamp-col", default=None, help="Timestamp column name (auto-detect if not provided)")
    parser.add_argument("--dedupe-on", default=None, help="Comma-separated columns to dedupe on (defaults to timestamp column)")
    parser.add_argument("--batch-size", type=int, default=10000, help="Number of rows per CSV chunk to process")
    parser.add_argument("--encoding", default="utf-8", help="CSV encoding")
    parser.add_argument("--if-exists", choices=["append", "replace"], default="append", help="Behavior if table exists")
    args = parser.parse_args()

    print(f"Starting loader: input_dir={args.input_dir}, pattern={args.pattern}, db_path={args.db_path}, table={args.table}")

    input_glob = os.path.join(args.input_dir, args.pattern)
    files = sorted(glob.glob(input_glob))
    print(f"Found {len(files)} files matching pattern")
    if not files:
        print(f"No files found matching: {input_glob}")
        return

    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)
    print(f"Creating/connecting to database at: {args.db_path}")
    conn = sqlite3.connect(args.db_path)

    header_schema = None
    columns_order = None
    dedupe_cols = None

    for fpath in files:
        print(f"Processing: {fpath}")
        try:
            # detect header line and build column names
            header_idx, header_line = _find_header_line_index(fpath, encoding=args.encoding)
            if header_line is None:
                print(f"Could not find a header line in {fpath}; skipping file")
                continue
            # clean header (remove leading '#', trim whitespace and split by comma)
            header_clean = header_line.lstrip("#").strip()
            col_names = [c.strip().strip('"') for c in header_clean.split(",")]
            skiprows = header_idx + 1  # skip header line itself since names are provided

            reader = pd.read_csv(fpath, names=col_names, header=None, skiprows=skiprows, chunksize=args.batch_size, encoding=args.encoding)

            for chunk in reader:
                # drop fully-empty rows (rare)
                if chunk.shape[0] == 0:
                    continue

                if columns_order is None:
                    columns_order = list(chunk.columns)
                    # detect timestamp
                    ts_col = args.timestamp_col or detect_timestamp_col(chunk)
                    if ts_col is None:
                        print("Could not detect a timestamp column; proceeding without timestamp parsing")
                    else:
                        args.timestamp_col = ts_col
                    # infer schema on this first chunk
                    header_schema = infer_schema_from_df(chunk)
                    create_table_if_needed(conn, args.table, header_schema, if_exists=args.if_exists)
                    # determine dedupe columns
                    if args.dedupe_on:
                        dedupe_cols = [c.strip() for c in args.dedupe_on.split(",") if c.strip()]
                    else:
                        dedupe_cols = [args.timestamp_col] if args.timestamp_col else []
                    create_unique_index(conn, args.table, dedupe_cols)

                # parse timestamp column if available
                if args.timestamp_col and args.timestamp_col in chunk.columns:
                    chunk[args.timestamp_col] = pd.to_datetime(chunk[args.timestamp_col], errors="coerce", utc=True)
                    # store as ISO string
                    chunk[args.timestamp_col] = chunk[args.timestamp_col].dt.strftime("%Y-%m-%dT%H:%M:%S%z")

                # ensure all expected columns present
                for c in columns_order:
                    if c not in chunk.columns:
                        chunk[c] = None

                rows = rows_from_df(chunk, columns_order)
                if not rows:
                    continue

                placeholders = ",".join(["?" for _ in columns_order])
                cols_sql = ",".join([f'\"{c}\"' for c in columns_order])
                insert_sql = f'INSERT OR IGNORE INTO \"{args.table}\" ({cols_sql}) VALUES ({placeholders})'

                cur = conn.cursor()
                cur.executemany(insert_sql, rows)
                conn.commit()
        except Exception as e:
            import traceback

            print(f"Error processing file {fpath}: {e}")
            traceback.print_exc()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()

