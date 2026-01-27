#!/usr/bin/env python3
import sqlite3
from pathlib import Path

DB = Path(r"c:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\kelmarsh_all_data.db")
if not DB.exists():
    print("DB not found:", DB)
    raise SystemExit(1)

conn = sqlite3.connect(str(DB))
cur = conn.cursor()
# list tables
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
print('Tables:', tables)

for t in tables:
    print('\nTable:', t)
    cur.execute(f"PRAGMA table_info('{t}')")
    cols = [(r[1], r[2]) for r in cur.fetchall()]
    print('Columns (name,type):')
    for c in cols:
        print(' ', c)
    # sample a few rows
    cur.execute(f"SELECT rowid, * FROM '{t}' LIMIT 5")
    rows = cur.fetchall()
    print('\nSample rows:')
    for r in rows:
        print(' ', r)

conn.close()
print('\nDone')

