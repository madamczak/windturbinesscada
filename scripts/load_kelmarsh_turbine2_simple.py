"""
Simpler loader for Turbine_Data_Kelmarsh_2_*.csv -> sqlite using only stdlib (csv, sqlite3).
This avoids pandas tokenization issues and will create data/sqlitedbs/kelmarsh_2_data.db.

Behavior:
- Detect header line starting with '# Date and time' or first non-# line with commas.
- Use header fields as column names (strip quotes).
- Create a table with all TEXT columns.
- Create a UNIQUE index on the detected timestamp column (if any) to dedupe.
- Insert rows with executemany in batches per file.

Usage:
python scripts\load_kelmarsh_turbine2_simple.py --input-dir data/kelmarsh_data --pattern "Turbine_Data_Kelmarsh_2_*.csv" --db-path data/sqlitedbs/kelmarsh_2_data.db --table kelmarsh_2
"""

import argparse
import glob
import os
import sqlite3
import csv
from itertools import islice


def find_header(fpath, encoding="utf-8"):
    with open(fpath, "r", encoding=encoding, errors="replace") as fh:
        for i, line in enumerate(fh):
            if line.lstrip().startswith("#") and "Date and time" in line:
                return i, line.lstrip("#").strip()
        fh.seek(0)
        for i, line in enumerate(fh):
            if "," in line and not line.lstrip().startswith("#"):
                return i, line.strip()
    return None, None


def chunks(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk


def sanitize_colname(c):
    return c.strip().strip('"').strip().replace('\n', '').replace('\r', '')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="data/kelmarsh_data")
    parser.add_argument("--pattern", default="Turbine_Data_Kelmarsh_2_*.csv")
    parser.add_argument("--db-path", default="data/sqlitedbs/kelmarsh_2_data.db")
    parser.add_argument("--table", default="kelmarsh_2")
    parser.add_argument("--batch-size", type=int, default=5000)
    args = parser.parse_args()

    files = sorted(glob.glob(os.path.join(args.input_dir, args.pattern)))
    print(f"Found {len(files)} files")
    if not files:
        return

    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)
    conn = sqlite3.connect(args.db_path)
    cur = conn.cursor()

    table_created = False
    timestamp_col = None

    for f in files:
        print(f"Processing {f}")
        header_idx, header_line = find_header(f)
        if header_line is None:
            print("  No header found, skipping")
            continue
        col_names = [sanitize_colname(c) for c in header_line.split(",")]
        # determine timestamp column
        for candidate in ("Date and time", "Date and Time", "Date and time", "timestamp", "Timestamp", "date", "Date"):
            if candidate in col_names:
                timestamp_col = candidate
                break
        # create table if not yet
        if not table_created:
            cols_sql = ", ".join([f'"{c}" TEXT' for c in col_names])
            sql = f'CREATE TABLE IF NOT EXISTS "{args.table}" ({cols_sql})'
            cur.execute(sql)
            conn.commit()
            if timestamp_col:
                idx_name = f'ux_{args.table}_{timestamp_col.replace(" ","_")}'
                cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}" ON "{args.table}" ("{timestamp_col}")')
                conn.commit()
            table_created = True

        # open file and iterate lines after header_idx
        with open(f, "r", encoding="utf-8", errors="replace") as fh:
            # skip header_idx+1 lines
            for _ in range(header_idx + 1):
                next(fh, None)
            reader = csv.reader(fh)
            batch = []
            for row in reader:
                # if row length doesn't match header, try joining remainder
                if len(row) < len(col_names):
                    # pad
                    row += [None] * (len(col_names) - len(row))
                elif len(row) > len(col_names):
                    # truncate extra
                    row = row[: len(col_names)]
                batch.append(tuple([v if v != "" else None for v in row]))
                if len(batch) >= args.batch_size:
                    placeholders = ",".join(["?" for _ in col_names])
                    cols_sql = ",".join([f'"{c}"' for c in col_names])
                    cur.executemany(f'INSERT OR IGNORE INTO "{args.table}" ({cols_sql}) VALUES ({placeholders})', batch)
                    conn.commit()
                    batch = []
            if batch:
                placeholders = ",".join(["?" for _ in col_names])
                cols_sql = ",".join([f'"{c}"' for c in col_names])
                cur.executemany(f'INSERT OR IGNORE INTO "{args.table}" ({cols_sql}) VALUES ({placeholders})', batch)
                conn.commit()
    conn.close()
    print("Finished")


if __name__ == "__main__":
    main()

