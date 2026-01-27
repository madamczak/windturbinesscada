#!/usr/bin/env python3
"""
Export turbine_1..turbine_15 tables from a source DB into a single destination DB.
If turbine_X tables don't exist in source, create them in dest by selecting from the
combined table (detected heuristically) using the Turbine column.

Usage:
  python scripts/export_turbines_to_db.py \
      --src data/sqlitedbs/penmanshiel_all_data.db \
      --dst data/sqlitedbs/penmanshiel_by_turbine.db

Logs each step to the console.
"""
from __future__ import annotations
import argparse
import shutil
import sqlite3
from pathlib import Path
from datetime import datetime
import sys


def detect_combined_table(conn: sqlite3.Connection) -> str | None:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    names = [r[0] for r in cur.fetchall()]
    for name in names:
        if '_all' in name.lower():
            return name
    for name in names:
        ln = name.lower()
        if 'penmanshiel' in ln or 'kelmarsh' in ln or 'combined' in ln:
            return name
    return None


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (table,))
    return cur.fetchone() is not None


def backup_if_exists(path: Path) -> None:
    if path.exists():
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        bak = path.with_name(path.name + '.bak.' + ts)
        print(f'Backing up existing destination DB {path} -> {bak}')
        shutil.copy2(path, bak)


def export_tables(src: Path, dst: Path, turbines=range(1,16)):
    src_p = src.resolve()
    dst_p = dst.resolve()
    print('Source DB:', src_p)
    print('Destination DB:', dst_p)

    backup_if_exists(dst_p)

    conn = sqlite3.connect(str(src_p))
    try:
        combined = detect_combined_table(conn)
        if not combined:
            print('Could not detect a combined table in source DB. Aborting.')
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            print('Available tables:')
            for r in cur.fetchall():
                print('  ', r[0])
            return 1

        print('Detected combined table:', combined)

        # Attach destination DB
        print('Attaching destination DB...')
        conn.execute(f"ATTACH DATABASE ? AS dst", (str(dst_p),))

        for i in turbines:
            tbl = f"turbine_{i}"
            print('\n-- Processing', tbl)
            if table_exists(conn, tbl):
                print(f"Source has table {tbl}; copying to destination")
                conn.execute(f'DROP TABLE IF EXISTS dst."{tbl}"')
                conn.execute(f'CREATE TABLE dst."{tbl}" AS SELECT * FROM main."{tbl}"')
            else:
                print(f"Source missing table {tbl}; creating from combined table using Turbine filter")
                num2 = f"{i:02d}"
                num1 = str(i)
                where = (
                    f"TRIM(Turbine) = '{num2}' OR TRIM(Turbine) = '{num1}' OR (TRIM(Turbine) != '' AND CAST(TRIM(Turbine) AS INTEGER) = {i})"
                )
                conn.execute(f'DROP TABLE IF EXISTS dst."{tbl}"')
                conn.execute(f'CREATE TABLE dst."{tbl}" AS SELECT * FROM main."{combined}" WHERE ({where})')
            # print count
            cur = conn.cursor()
            cur.execute(f'SELECT COUNT(*) FROM dst."{tbl}"')
            cnt = cur.fetchone()[0]
            print(f' -> {tbl} rows in destination: {cnt}')

        # Detach destination
        conn.execute('DETACH DATABASE dst')
        print('\nAll done. Destination DB should contain turbine_1..turbine_15 tables.')
        return 0
    finally:
        conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', default='data/sqlitedbs/penmanshiel_all_data.db')
    parser.add_argument('--dst', default='data/sqlitedbs/penmanshiel_by_turbine.db')
    args = parser.parse_args(argv)

    src = Path(args.src)
    dst = Path(args.dst)

    if not src.exists():
        print('Source DB not found:', src)
        return 2

    rc = export_tables(src, dst)
    if rc == 0:
        print('\nVerifying destination DB contents...')
        # quick verification
        dconn = sqlite3.connect(str(dst.resolve()))
        try:
            dcur = dconn.cursor()
            total = 0
            for i in range(1,16):
                tbl = f'turbine_{i}'
                dcur.execute(f'SELECT COUNT(*) FROM "{tbl}"')
                cnt = dcur.fetchone()[0]
                print(f'  {tbl}: {cnt}')
                total += cnt
            print('Total rows across turbine tables in destination:', total)
        finally:
            dconn.close()
    return rc


if __name__ == '__main__':
    raise SystemExit(main())

