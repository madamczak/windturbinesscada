#!/usr/bin/env python3
"""Delete rows before 2016-03-01T00:00:00+0000 from all turbine tables in
data/sqlitedbs/data_by_turbine/kelmarsh_data_by_turbine.db

This script attempts several common datetime storage formats per column:
- numeric epoch stored in seconds or milliseconds
- ISO-like text parsed by SQLite's strftime('%s', col)
- simple lexicographic comparison against an ISO-like timestamp (no tz)

It logs each step to the console.
"""
import sqlite3
from pathlib import Path
import re
import datetime
import sys

DB = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\kelmarsh_data_by_turbine.db")
TARGET_ISO = '2016-03-01T00:00:00+0000'

def parse_target_ms(iso_str):
    s = iso_str
    # make +0000 -> +00:00 for python parsing
    if s.endswith('+0000'):
        s = s[:-5] + '+00:00'
    try:
        dt = datetime.datetime.fromisoformat(s)
        # ensure timezone-aware in UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        dt_utc = dt.astimezone(datetime.timezone.utc)
        return int(dt_utc.timestamp() * 1000), dt_utc.strftime('%Y-%m-%dT%H:%M:%S')
    except Exception as e:
        print('Failed to parse target iso:', iso_str, '->', e)
        raise


def main():
    if not DB.exists():
        print('DB not found:', DB)
        sys.exit(2)

    target_ms, iso_no_tz = parse_target_ms(TARGET_ISO)
    print('Target cutoff (ms):', target_ms, 'iso_no_tz:', iso_no_tz)

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # turbines 1..6
    for t in range(1, 7):
        tbl = f"turbine_{t}"
        print('\nProcessing table', tbl)
        try:
            cur.execute(f"PRAGMA table_info('{tbl}')")
            cols = [r[1] for r in cur.fetchall()]
        except Exception as e:
            print('  ERROR reading table info for', tbl, ':', e)
            continue
        if not cols:
            print('  No columns found for', tbl)
            continue
        cand_cols = [c for c in cols if re.search(r"timestamp|datetime|date|time|created_at|ts", c, re.IGNORECASE)]
        if not cand_cols:
            print('  No timestamp-like columns found; skipping')
            continue
        print('  Timestamp-like columns:', cand_cols)

        conds = []
        params = []
        for col in cand_cols:
            # safe quoting of column name by double quotes
            col_quoted = '"' + col.replace('"', '""') + '"'
            # build condition tries several formats; compare in milliseconds
            cond = f"((CAST({col_quoted} AS INTEGER) < ?) OR ((CAST({col_quoted} AS INTEGER) * 1000) < ?) OR ((strftime('%s', {col_quoted}) IS NOT NULL) AND (strftime('%s', {col_quoted}) * 1000 < ?)) OR ({col_quoted} < ?))"
            conds.append(cond)
            params.extend([target_ms, target_ms, target_ms, iso_no_tz])

        where = ' OR '.join(conds)
        count_sql = f"SELECT COUNT(*) as c FROM '{tbl}' WHERE {where}"
        try:
            cur.execute(count_sql, params)
            row = cur.fetchone()
            cnt = row['c'] if row else 0
            print(f'  Matching rows (before delete): {cnt}')
        except Exception as e:
            print('  ERROR counting matching rows for', tbl, ':', e)
            continue

        if cnt <= 0:
            print('  Nothing to delete for', tbl)
            continue

        delete_sql = f"DELETE FROM '{tbl}' WHERE {where}"
        try:
            cur.execute(delete_sql, params)
            conn.commit()
            print('  Deleted rows:', cur.rowcount)
        except Exception as e:
            print('  ERROR deleting rows for', tbl, ':', e)
            conn.rollback()
            continue

    cur.close()
    conn.close()
    print('\nDone.')

if __name__ == '__main__':
    main()

