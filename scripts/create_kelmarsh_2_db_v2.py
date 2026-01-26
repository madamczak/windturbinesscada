"""
Verbose stdlib CSV -> sqlite loader for Turbine_Data_Kelmarsh_2_*.csv
This script logs progress to stdout and creates the sqlite DB with table `kelmarsh_2`.

Usage:
python scripts\create_kelmarsh_2_db_v2.py
"""
import os
import glob
import sqlite3
import csv
import sys

INPUT_DIR = "data/kelmarsh_data"
PATTERN = r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\kelmarsh_data\Turbine_Data_Kelmarsh_3_*.csv"
DB_PATH = "data/sqlitedbs/kelmarsh_3_data.db"
TABLE = "kelmarsh_3"
BATCH = 1000


def find_header(fpath):
    with open(fpath, "r", encoding="utf-8", errors="replace") as fh:
        for i, line in enumerate(fh):
            if line.lstrip().startswith("#") and "Date and time" in line:
                return i, line.lstrip("#").strip()
        fh.seek(0)
        for i, line in enumerate(fh):
            if "," in line and not line.lstrip().startswith("#"):
                return i, line.strip()
    return None, None


def sanitize(c):
    return c.strip().strip('"').replace('\n','').replace('\r','')


def main():
    print(f"Script: create_kelmarsh_2_db_v2.py")
    files = sorted(glob.glob(os.path.join(INPUT_DIR, PATTERN)))
    print(f"Found {len(files)} files matching {PATTERN} in {INPUT_DIR}")
    if not files:
        return

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    table_created = False
    for f in files:
        print(f"Processing file: {f}")
        header_idx, header_line = find_header(f)
        if header_line is None:
            print("  No header found; skipping")
            continue
        col_names = [sanitize(x) for x in header_line.split(",")]
        print(f"  Detected {len(col_names)} columns")
        if not table_created:
            cols_sql = ", ".join([f'"{c}" TEXT' for c in col_names])
            sql = f'CREATE TABLE IF NOT EXISTS "{TABLE}" ({cols_sql})'
            cur.execute(sql)
            conn.commit()
            # try to create unique index on first column (Date and time) if present
            first = col_names[0]
            try:
                idx_name = f'ux_{TABLE}_{first.replace(" ","_")}'
                cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}" ON "{TABLE}" ("{first}")')
                conn.commit()
                print(f"  Created unique index on column: {first}")
            except Exception as e:
                print(f"  Could not create index: {e}")
            table_created = True

        with open(f, "r", encoding="utf-8", errors="replace") as fh:
            for _ in range(header_idx + 1):
                next(fh, None)
            reader = csv.reader(fh)
            batch = []
            rownum = 0
            for row in reader:
                rownum += 1
                if len(row) < len(col_names):
                    row += [None] * (len(col_names) - len(row))
                elif len(row) > len(col_names):
                    row = row[:len(col_names)]
                batch.append(tuple([v if v != "" else None for v in row]))
                if len(batch) >= BATCH:
                    placeholders = ",".join(["?" for _ in col_names])
                    cols_sql = ",".join([f'"{c}"' for c in col_names])
                    cur.executemany(f'INSERT OR IGNORE INTO "{TABLE}" ({cols_sql}) VALUES ({placeholders})', batch)
                    conn.commit()
                    print(f"  Inserted {rownum} rows (plus earlier files)")
                    batch = []
            if batch:
                placeholders = ",".join(["?" for _ in col_names])
                cols_sql = ",".join([f'"{c}"' for c in col_names])
                cur.executemany(f'INSERT OR IGNORE INTO "{TABLE}" ({cols_sql}) VALUES ({placeholders})', batch)
                conn.commit()
                print(f"  Inserted final {len(batch)} rows for this file")

    conn.close()
    print(f"Done. DB created at: {DB_PATH}")


if __name__ == '__main__':
    main()

