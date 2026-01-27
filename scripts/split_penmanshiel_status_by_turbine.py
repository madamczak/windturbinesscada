#!/usr/bin/env python3
"""
Split penmanshiel_all_status.db into per-turbine tables turbine_1..turbine_15 in a
new DB file data/sqlitedbs/penmanshiel_status_by_turbine.db. Logs each step.

Usage:
  python scripts/split_penmanshiel_status_by_turbine.py \
      --src data/sqlitedbs/penmanshiel_all_status.db \
      --dst data/sqlitedbs/penmanshiel_status_by_turbine.db

Assumes the combined/status table is named "Penmanshiel Status" (the script will
try to detect it if not explicitly present).
"""
from __future__ import annotations
import argparse
import shutil
import sqlite3
from pathlib import Path
from datetime import datetime
import sys


def detect_table(conn: sqlite3.Connection, preferred_name: str = 'Penmanshiel Status') -> str | None:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    names = [r[0] for r in cur.fetchall()]
    if preferred_name in names:
        return preferred_name
    # try heuristics
    for n in names:
        ln = n.lower()
        if 'penmanshiel' in ln and 'status' in ln:
            return n
    for n in names:
        if 'status' in n.lower():
            return n
    return None


def backup_if_exists(path: Path) -> None:
    if path.exists():
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        bak = path.with_name(path.name + '.bak.' + ts)
        print(f'Backing up existing destination DB {path} -> {bak}')
        shutil.copy2(path, bak)


def split_status_db(src: Path, dst: Path, turbines=range(1, 16)):
    print('Source DB:', src)
    print('Destination DB:', dst)
    backup_if_exists(dst)

    conn = sqlite3.connect(str(src))
    try:
        table = detect_table(conn)
        if not table:
            print('Could not detect status table in source DB. Available tables:')
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            for r in cur.fetchall():
                print('  ', r[0])
            return 2
        print('Detected source table:', table)

        print('Beginning split operation...')
        cur = conn.cursor()
        cur.execute('BEGIN')
        try:
            # Attach destination as dst
            print('Attaching destination DB as dst')
            conn.execute('ATTACH DATABASE ? AS dst', (str(dst.resolve()),))

            # Drop any existing turbine tables in dst
            for i in turbines:
                tbl = f'turbine_{i}'
                print(f'Dropping dst.{tbl} if exists')
                conn.execute(f'DROP TABLE IF EXISTS dst."{tbl}"')
            print('Dropping dst.turbine_unknown if exists')
            conn.execute('DROP TABLE IF EXISTS dst."turbine_unknown"')

            # Create per-turbine tables
            for i in turbines:
                tbl = f'turbine_{i}'
                num = str(i)
                # WHERE clause: numeric match or string match
                where = f"(Turbine = {i}) OR (TRIM(Turbine) = '{num}') OR (CAST(TRIM(Turbine) AS INTEGER) = {i})"
                sql = f'CREATE TABLE dst."{tbl}" AS SELECT * FROM main."{table}" WHERE {where};'
                print(f'Creating dst.{tbl} with filter: {where}')
                conn.execute(sql)

            # Create turbine_unknown for rows not matching 1..15 or NULL/empty
            twodigits = ','.join([f"'{n}'" for n in [str(i) for i in turbines]])
            where_unknown = (
                f"(Turbine IS NULL) OR (TRIM(Turbine) = '') OR (CAST(TRIM(Turbine) AS INTEGER) < {min(turbines)} OR CAST(TRIM(Turbine) AS INTEGER) > {max(turbines)}) "
                f"AND TRIM(Turbine) NOT IN ({twodigits})"
            )
            print('Creating dst.turbine_unknown with filter:', where_unknown)
            conn.execute(f'CREATE TABLE dst."turbine_unknown" AS SELECT * FROM main."{table}" WHERE {where_unknown};')

            conn.execute('COMMIT')
            print('Transaction committed')
        except Exception as e:
            conn.execute('ROLLBACK')
            print('Transaction rolled back due to error:', e)
            raise
        finally:
            # Detach destination
            try:
                conn.execute('DETACH DATABASE dst')
                print('Detached destination DB')
            except Exception:
                pass

        # Verification
        print('\nVerification: counts in destination DB')
        dconn = sqlite3.connect(str(dst.resolve()))
        try:
            dcur = dconn.cursor()
            total = 0
            for i in turbines:
                tbl = f'turbine_{i}'
                dcur.execute(f'SELECT COUNT(*) FROM "{tbl}"')
                cnt = dcur.fetchone()[0]
                print(f'  {tbl}: {cnt}')
                total += cnt
            dcur.execute('SELECT COUNT(*) FROM "turbine_unknown"')
            unk = dcur.fetchone()[0]
            print(f'  turbine_unknown: {unk}')
            print('Total in destination (incl unknown):', total + unk)
        finally:
            dconn.close()

        return 0
    finally:
        conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', default='data/sqlitedbs/penmanshiel_all_status.db')
    parser.add_argument('--dst', default='data/sqlitedbs/penmanshiel_status_by_turbine.db')
    args = parser.parse_args(argv)

    src = Path(args.src)
    dst = Path(args.dst)

    if not src.exists():
        print('Source DB not found:', src)
        return 2

    rc = split_status_db(src, dst)
    return rc


if __name__ == '__main__':
    raise SystemExit(main())

