#!/usr/bin/env python3
"""
Split a combined penmanshiel/kelmarsh SQLite DB into separate turbine_N tables.

Usage:
  python scripts/split_penmanshiel_db.py /path/to/penmanshiel_all_data.db

If no DB path is provided the script will search common locations under
`data/sqlitedbs/` and `sqlitedbs/` for files containing "penmanshiel".

Behavior:
- Backs up the original DB to <db>.bak.TIMESTAMP
- Detects a combined table (table name containing "_all" or "penmanshiel" / "kelmarsh")
- For turbines 1..15 creates tables named `turbine_1` .. `turbine_15` containing rows
  where the Turbine column corresponds to that turbine (handles '01' and '1')
- Creates `turbine_unknown` for unmatched/NULL turbine values
- Idempotent: drops existing turbine_* tables before creating them
- Prints counts per created table and overall validation totals
"""

from __future__ import annotations
import argparse
import shutil
import sqlite3
from pathlib import Path
from datetime import datetime
import sys
import glob


def find_candidate_db(provided: str | None) -> Path | None:
    if provided:
        p = Path(provided)
        if p.exists():
            return p
        return None
    # search common locations
    patterns = [
        "data/sqlitedbs/*penmanshiel*.db",
        "sqlitedbs/*penmanshiel*.db",
        "data/sqlitedbs/*kelmarsh*.db",
        "sqlitedbs/*kelmarsh*.db",
        "data/sqlitedbs/*.db",
        "sqlitedbs/*.db",
        "*.db",
    ]
    for pat in patterns:
        for path in Path('.').glob(pat):
            return path
    return None


def detect_combined_table(conn: sqlite3.Connection) -> str | None:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    names = [r[0] for r in cur.fetchall()]
    # prefer tables containing '_all'
    for name in names:
        lname = name.lower()
        if '_all' in lname:
            return name
    # fallback heuristics
    for name in names:
        lname = name.lower()
        if 'penmanshiel' in lname or 'kelmarsh' in lname or 'combined' in lname:
            return name
    return None


def backup_db(db_path: Path) -> Path:
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    bak = db_path.with_name(db_path.name + '.bak.' + ts)
    shutil.copy2(db_path, bak)
    return bak


def create_turbine_tables(conn: sqlite3.Connection, combined_table: str, turbines: range):
    cur = conn.cursor()
    print(f"Beginning split: creating turbine tables from '{combined_table}'")
    # Use a transaction so the operation is atomic
    cur.execute('BEGIN')
    try:
        # Drop any existing turbine_X tables
        for i in turbines:
            tbl = f"turbine_{i}"
            print(f"Dropping table if exists: {tbl}")
            cur.execute(f"DROP TABLE IF EXISTS \"{tbl}\"")
        print('Dropping table if exists: turbine_unknown')
        cur.execute('DROP TABLE IF EXISTS "turbine_unknown"')

        # Create tables per turbine
        for i in turbines:
            tbl = f"turbine_{i}"
            num2 = f"{i:02d}"
            num1 = str(i)
            # Build WHERE clause to match '01' and '1' and numeric cast
            where = (
                f"TRIM(Turbine) = '{num2}' OR TRIM(Turbine) = '{num1}' OR "
                f"(TRIM(Turbine) != '' AND CAST(TRIM(Turbine) AS INTEGER) = {i})"
            )
            sql = (
                f"CREATE TABLE \"{tbl}\" AS SELECT * FROM \"{combined_table}\" WHERE ({where});"
            )
            print(f"Creating table {tbl} with filter: {where}")
            cur.execute(sql)

        # Create turbine_unknown for rows not matched by 1..15 or with NULL Turbine
        # We treat as unknown when cast to integer not in 1..15 and text trimmed not in the two-digit forms
        twodigits = ",".join([f"'{n:02d}'" for n in turbines])
        where_unknown = (
            f"(Turbine IS NULL) OR (TRIM(Turbine) = '') OR (CAST(TRIM(Turbine) AS INTEGER) < {min(turbines)} OR CAST(TRIM(Turbine) AS INTEGER) > {max(turbines)}) "
            f"AND TRIM(Turbine) NOT IN ({twodigits})"
        )
        print(f"Creating table turbine_unknown with filter: {where_unknown}")
        sql_unknown = f"CREATE TABLE \"turbine_unknown\" AS SELECT * FROM \"{combined_table}\" WHERE {where_unknown};"
        cur.execute(sql_unknown)

        cur.execute('COMMIT')
        print('Transaction committed successfully')
    except Exception:
        cur.execute('ROLLBACK')
        print('Transaction rolled back due to error')
        raise


def print_counts(conn: sqlite3.Connection, turbines: range):
    cur = conn.cursor()
    total = 0
    print('\nTable counts:')
    for i in turbines:
        tbl = f"turbine_{i}"
        try:
            cur.execute(f'SELECT COUNT(*) FROM "{tbl}"')
            cnt = cur.fetchone()[0]
        except sqlite3.OperationalError:
            cnt = 0
        print(f'  {tbl}: {cnt}')
        total += cnt
    # unknown
    try:
        cur.execute('SELECT COUNT(*) FROM "turbine_unknown"')
        unk = cur.fetchone()[0]
    except sqlite3.OperationalError:
        unk = 0
    print(f'  turbine_unknown: {unk}')
    print(f'\nTotal rows placed into turbine tables: {total + unk}\n')


def main(argv=None):
    parser = argparse.ArgumentParser(description='Split combined DB into turbine_N tables')
    parser.add_argument('db', nargs='?', help='Path to SQLite DB file')
    parser.add_argument('--min', type=int, default=1, help='Minimum turbine number (default 1)')
    parser.add_argument('--max', type=int, default=15, help='Maximum turbine number (default 15)')
    parser.add_argument('--no-backup', action='store_true', help='Do not create a backup copy')
    args = parser.parse_args(argv)

    db_path = find_candidate_db(args.db)
    if not db_path:
        print('No database found. Provide a path or place penmanshiel DB in data/sqlitedbs/ or sqlitedbs/.')
        # print candidates for user
        candidates = list(Path('data/sqlitedbs').glob('*.db')) if Path('data/sqlitedbs').exists() else []
        candidates += list(Path('sqlitedbs').glob('*.db')) if Path('sqlitedbs').exists() else []
        if candidates:
            print('Found DB candidates:')
            for c in candidates:
                print('  ', c)
        sys.exit(2)

    print('Using DB:', db_path)
    if not args.no_backup:
        bak = backup_db(db_path)
        print('Backup created at', bak)

    print('Opening SQLite connection...')
    conn = sqlite3.connect(str(db_path))
    try:
        combined = detect_combined_table(conn)
        if not combined:
            print('Could not detect combined table. Available tables:')
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            for r in cur.fetchall():
                print('  ', r[0])
            conn.close()
            sys.exit(3)
        print('Detected combined table:', combined)

        turbines = range(args.min, args.max + 1)
        print(f"Splitting into turbine tables for turbines {args.min}..{args.max}")
        create_turbine_tables(conn, combined, turbines)
        print_counts(conn, turbines)
        print('Done.')
    finally:
        conn.close()


if __name__ == '__main__':
    main()

