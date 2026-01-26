#!/usr/bin/env python3
"""First approach to joining status-interval DBs with time-series data DBs.

Approach summary (implemented):
- Status DB contains intervals: start_time, end_time, duration (one row per interval).
- Data DB contains timestamped measurements every fixed interval (e.g., 10 minutes).

First approach algorithm:
1. Detect the status table and its start/end columns (auto-detect common names).
2. Detect the data table and its timestamp column (auto-detect common names).
3. Load status intervals and data timestamps into pandas, parse datetimes.
4. Use pandas.merge_asof to assign each data timestamp the most recent status interval with start <= timestamp.
   Then filter those joined rows to ensure timestamp < end (i.e., interval contains the timestamp).

This is efficient for large tables because merge_asof is O(n) after sorting. It's a practical first approach.

The script writes a new sqlite database with a table named 'joined_first_approach' containing the
data timestamps and the columns from the matched status interval (prefixed by 'status_').

Usage:
    python scripts\first_approach_join.py
    python scripts\first_approach_join.py --status-db data/sqlitedbs/penmanshiel_all_status.db \
        --data-db data/sqlitedbs/penmanshiel_all_data.db \
        --out-db data/sqlitedbs\first_approach_to_join.db

"""

import argparse
import sqlite3
from pathlib import Path
import pandas as pd
import sys
import textwrap

DEFAULT_STATUS_DB = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\penmanshiel_all_status.db")
DEFAULT_DATA_DB = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\penmanshiel_all_data.db")
DEFAULT_OUT_DB = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\first_approach_to_join.db")

# common timestamp candidates
TS_CANDIDATES = [
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
    "Date and Time",
]


def first_user_table(conn: sqlite3.Connection):
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 1"
    )
    row = cur.fetchone()
    return row[0] if row else None


def detect_ts_column(cols):
    for c in TS_CANDIDATES:
        if c in cols:
            return c
    # fallback: first column
    return cols[0] if cols else None


def pick_start_end_columns(cols):
    # heuristics to pick start/end columns from status table columns
    low = [c.lower() for c in cols]
    start_candidates = [
        'start', 'start_time', 'starttime', 'from', 'begin', 'begin_time', 'datetime_start'
    ]
    end_candidates = [
        'end', 'end_time', 'endtime', 'to', 'finish', 'finish_time', 'datetime_end'
    ]
    start = None
    end = None
    for cand in start_candidates:
        for i, c in enumerate(low):
            if cand == c or cand in c:
                start = cols[i]
                break
        if start:
            break
    for cand in end_candidates:
        for i, c in enumerate(low):
            if cand == c or cand in c:
                end = cols[i]
                break
        if end:
            break
    # if not found, try first two columns
    if not start and len(cols) >= 1:
        start = cols[0]
    if not end and len(cols) >= 2:
        end = cols[1]
    return start, end


def load_table_columns(db_path: Path):
    conn = sqlite3.connect(str(db_path))
    try:
        tbl = first_user_table(conn)
        if not tbl:
            raise RuntimeError(f"No user table found in {db_path}")
        # escape any single quotes in the table name for safe PRAGMA usage
        qtbl = tbl.replace("'", "''")
        cur = conn.execute(f"PRAGMA table_info('{qtbl}')")
        cols = [r[1] for r in cur.fetchall()]
        return tbl, cols
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Join status-interval DB to time-series data DB (first approach)")
    parser.add_argument('--status-db', type=Path, default=DEFAULT_STATUS_DB)
    parser.add_argument('--data-db', type=Path, default=DEFAULT_DATA_DB)
    parser.add_argument('--out-db', type=Path, default=DEFAULT_OUT_DB)
    parser.add_argument('--status-table', type=str, default=None)
    parser.add_argument('--data-table', type=str, default=None)
    parser.add_argument('--interval-minutes', type=int, default=10,
                        help='expected interval of data timestamps (used for reporting only)')
    args = parser.parse_args()

    print('First approach: map each data timestamp to the status interval active at that time')

    # validate files
    if not args.status_db.exists():
        print(f"Status DB not found: {args.status_db}")
        return 2
    if not args.data_db.exists():
        print(f"Data DB not found: {args.data_db}")
        return 2

    # detect tables and columns
    try:
        if args.status_table:
            status_table = args.status_table
            conn_s = sqlite3.connect(str(args.status_db))
            qstatus = status_table.replace("'", "''")
            cur = conn_s.execute(f"PRAGMA table_info('{qstatus}')")
            status_cols = [r[1] for r in cur.fetchall()]
            conn_s.close()
        else:
            status_table, status_cols = load_table_columns(args.status_db)

        if args.data_table:
            data_table = args.data_table
            conn_d = sqlite3.connect(str(args.data_db))
            qdata = data_table.replace("'", "''")
            cur = conn_d.execute(f"PRAGMA table_info('{qdata}')")
            data_cols = [r[1] for r in cur.fetchall()]
            conn_d.close()
        else:
            data_table, data_cols = load_table_columns(args.data_db)
    except Exception as e:
        print(f"Error detecting tables/columns: {e}")
        return 2

    # escape table names for SQL usage
    qstatus = status_table.replace("'", "''")
    qdata = data_table.replace("'", "''")

    print(f"Status DB: {args.status_db} -> table `{status_table}` with columns: {status_cols[:6]}{('...' if len(status_cols)>6 else '')}")
    print(f"Data DB: {args.data_db} -> table `{data_table}` with columns: {data_cols[:6]}{('...' if len(data_cols)>6 else '')}")

    start_col, end_col = pick_start_end_columns(status_cols)
    ts_col = detect_ts_column(data_cols)

    print(f"Detected status start column: {start_col}, end column: {end_col}")
    print(f"Detected data timestamp column: {ts_col}")

    # load status intervals
    conn_status = sqlite3.connect(str(args.status_db))
    conn_data = sqlite3.connect(str(args.data_db))

    try:
        status_df = pd.read_sql_query(f"SELECT * FROM '{qstatus}'", conn_status)
        if status_df.empty:
            print("Status table is empty")
            return 2

        # parse datetimes
        status_df[start_col] = pd.to_datetime(status_df[start_col], errors='coerce')
        status_df[end_col] = pd.to_datetime(status_df[end_col], errors='coerce')
        status_df = status_df.dropna(subset=[start_col])
        status_df = status_df.sort_values(by=start_col).reset_index(drop=True)

        # load data timestamps
        # try to select only timestamp column
        data_sel_sql = f"SELECT DISTINCT \"{ts_col}\" as ts FROM '{qdata}'"
        data_ts_df = pd.read_sql_query(data_sel_sql, conn_data)
        if data_ts_df.empty:
            print("Data table has no timestamps / is empty")
            return 2
        data_ts_df['ts'] = pd.to_datetime(data_ts_df['ts'], errors='coerce')
        data_ts_df = data_ts_df.dropna(subset=['ts']).sort_values('ts').reset_index(drop=True)

        print(f"Loaded {len(status_df)} status intervals and {len(data_ts_df)} distinct data timestamps")

        # merge_asof: find most recent status.start <= ts
        merged = pd.merge_asof(data_ts_df, status_df, left_on='ts', right_on=start_col, direction='backward', suffixes=('', '_status'))
        # filter rows where ts < end (if end exists)
        if end_col in merged.columns:
            within = merged[end_col].isna() | (merged['ts'] < merged[end_col])
            merged = merged[within].reset_index(drop=True)
        else:
            # if no end column, keep all merged
            merged = merged.reset_index(drop=True)

        print(f"After filtering by interval containment: {len(merged)} timestamps matched to status intervals")

        # prepare output df: keep ts and status columns (prefix status_)
        status_cols_keep = [c for c in status_df.columns if c not in [start_col, end_col]]
        out_df = merged[['ts', start_col, end_col] + status_cols_keep].copy()
        # rename columns to clear names
        out_df = out_df.rename(columns={ 'ts': 'timestamp', start_col: 'status_start', end_col: 'status_end' })
        # prefix other status cols
        rename_map = {}
        for c in status_cols_keep:
            rename_map[c] = f'status_{c}'
        out_df = out_df.rename(columns=rename_map)

        # write to output sqlite
        args.out_db.parent.mkdir(parents=True, exist_ok=True)
        conn_out = sqlite3.connect(str(args.out_db))
        try:
            out_df.to_sql('joined_first_approach', conn_out, if_exists='replace', index=False)
            # create index on timestamp
            conn_out.execute("CREATE INDEX IF NOT EXISTS idx_joined_timestamp ON joined_first_approach(timestamp)")
            conn_out.commit()
        finally:
            conn_out.close()

        print(f"Wrote {len(out_df)} rows to {args.out_db} -> table joined_first_approach")

        # print short stats
        sample = out_df.head(3).to_dict(orient='records')
        print("Sample rows:")
        for r in sample:
            print(textwrap.fill(str(r), width=200))

        return 0

    finally:
        conn_status.close()
        conn_data.close()


if __name__ == '__main__':
    sys.exit(main())

