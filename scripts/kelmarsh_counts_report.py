#!/usr/bin/env python3
"""Report total rows and rows before cutoff for turbine tables in kelmarsh per-turbine DB.
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

print('DB:', DB)
print('Cutoff:', CUT)
for t in range(1, 7):
    tbl = f"turbine_{t}"
    print('\nTable:', tbl)
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
        if not cur.fetchone():
            print('  Table not found, skipping')
            continue
        cur.execute(f"SELECT COUNT(*) FROM '{tbl}'")
        total = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM '{tbl}' WHERE \"Date and time\" < ?", (CUT,))
        before = cur.fetchone()[0]
        print('  Total rows:', total)
        print('  Rows before cutoff:', before)
    except Exception as e:
        print('  ERROR:', e)

cur.close()
conn.close()
print('\nDone.')

