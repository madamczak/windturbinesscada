#!/usr/bin/env python3
"""
Merge per-turbine Penmanshiel DBs (penmanshiel_XX_data.db) into a single SQLite DB.
Creates: data/sqlitedbs/penmanshiel_all_data.db with table name "Penmanshiel Data".

Usage: python scripts/merge_penmanshiel_dbs.py
"""
import os
import glob
import re
import sqlite3
import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SQLITE_DIR = os.path.join(BASE_DIR, 'data', 'sqlitedbs')
OUT_DB = os.path.join(SQLITE_DIR, 'penmanshiel_all_data.db')
PATTERN = os.path.join(SQLITE_DIR, 'penmanshiel_??_data.db')


def find_input_dbs():
    """Return sorted list of existing per-turbine DB paths for turbines 01..15."""
    files = sorted(glob.glob(PATTERN))
    # Filter for turbine numbers 01-15
    selected = []
    for p in files:
        m = re.search(r'penmanshiel_(\d{2})_data\.db$', os.path.basename(p), re.IGNORECASE)
        if not m:
            continue
        num = int(m.group(1))
        if 1 <= num <= 15:
            selected.append((num, p))
    selected.sort(key=lambda x: x[0])
    return selected


def detect_table_name(conn):
    """Detect a sensible data table name in the DB.
    Prefer exact 'Penmanshiel Data', else the only non-sqlite_ table.
    """
    q = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
    cur = conn.execute(q)
    tables = [r[0] for r in cur.fetchall()]
    if not tables:
        raise ValueError('No tables found in DB')
    # Preferred candidates
    for candidate in ["Penmanshiel Data", 'Penmanshiel_Data', 'penmanshiel_data', 'PenmanshielData']:
        if candidate in tables:
            return candidate
    if len(tables) == 1:
        return tables[0]
    # fallback: pick first table that contains 'data' in name
    for t in tables:
        if 'data' in t.lower():
            return t
    # otherwise return first
    return tables[0]


def read_table_from_db(db_path):
    """Read data table from the DB and return (df, detected_table_name)."""
    conn = sqlite3.connect(db_path)
    try:
        table = detect_table_name(conn)
        # Read entire table
        df = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
        return df, table
    finally:
        conn.close()


def merge_and_write(out_db, inputs):
    """Merge list of (turbine_num, db_path) and write to out_db."""
    frames = []
    seen_tables = set()
    for num, path in inputs:
        print(f'Reading turbine {num:02d} from {path}')
        try:
            df, table = read_table_from_db(path)
        except Exception as e:
            print(f'  ERROR reading {path}: {e}')
            continue
        print(f'  -> table "{table}" rows={len(df)} cols={len(df.columns)}')
        df['Turbine'] = f'{num:02d}'
        frames.append(df)
        seen_tables.add(table)

    if not frames:
        print('No data frames read, aborting.')
        return

    merged = pd.concat(frames, ignore_index=True)
    print(f'Merged rows={len(merged)} cols={len(merged.columns)}')

    # Write to output DB
    if os.path.exists(out_db):
        print(f'Removing existing output DB: {out_db}')
        os.remove(out_db)

    conn = sqlite3.connect(out_db)
    try:
        table_name = 'Penmanshiel Data'
        print(f'Writing merged table to {out_db} as "{table_name}"')
        merged.to_sql(table_name, conn, if_exists='replace', index=False)
        # create index on Turbine for convenience
        try:
            conn.execute('CREATE INDEX IF NOT EXISTS idx_turbine ON "{}" (Turbine)'.format(table_name))
        except Exception:
            pass
        print('Write complete.')
    finally:
        conn.close()


def main():
    inputs = find_input_dbs()
    expected = list(range(1, 16))
    found_nums = [n for n, _ in inputs]
    missing = [f'{n:02d}' for n in expected if n not in found_nums]
    if missing:
        print('Warning: missing turbine DBs for:', ', '.join(missing))
    print(f'Will process turbines: {", ".join([f"{n:02d}" for n,_ in inputs])}')
    merge_and_write(OUT_DB, inputs)


if __name__ == '__main__':
    main()

