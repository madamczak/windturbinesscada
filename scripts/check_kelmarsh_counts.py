#!/usr/bin/env python3
"""Delete rows before 2016-03-01 00:00:00 in kelmarsh per-turbine DB and print counts.

This script uses SQL like:
  DELETE FROM turbine_1 WHERE "Date and time" < '2016-03-01 00:00:00'

It prints counts before and after the delete for each turbine table.
"""
import sqlite3
from pathlib import Path

DB = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\kelmarsh_data_by_turbine.db")
CUT = '2016-03-01 00:00:00'

if not DB.exists():
    print('DB not found:', DB)
    raise SystemExit(2)

conn = sqlite3.connect(str(DB))
cur = conn.cursor()

for t in range(1, 7):
    tbl = f"turbine_{t}"
    print('\nTable:', tbl)
    try:
        # check if table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
        if not cur.fetchone():
            print('  Table not found, skipping')
            continue
        # count before
        try:
            cur.execute(f"SELECT COUNT(*) FROM '{tbl}' WHERE \"Date and time\" < ?", (CUT,))
            before = cur.fetchone()[0]
        except Exception as e:
            print('  Error counting rows by Date and time:', e)
            # try counting all rows to show total
            cur.execute(f"SELECT COUNT(*) FROM '{tbl}'")
            total = cur.fetchone()[0]
            print('  Total rows in table:', total)
            continue
        print('  Matching rows (before):', before)
        if before > 0:
            # perform delete
            try:
                cur.execute(f"DELETE FROM '{tbl}' WHERE \"Date and time\" < ?", (CUT,))
                conn.commit()
                print('  Deleted rows:', cur.rowcount)
            except Exception as e:
                print('  ERROR deleting rows:', e)
                conn.rollback()
                continue
            # count after
            cur.execute(f"SELECT COUNT(*) FROM '{tbl}' WHERE \"Date and time\" < ?", (CUT,))
            after = cur.fetchone()[0]
            print('  Matching rows (after):', after)
        else:
            print('  Nothing to delete')
    except Exception as e:
        print('  ERROR processing table:', e)

cur.close()
conn.close()
print('\nDone.')
