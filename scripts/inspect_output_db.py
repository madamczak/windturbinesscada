#!/usr/bin/env python3
import sqlite3
from pathlib import Path

p = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\kelmarsh_by_turbine.db")
print('Inspecting output DB:', p)
print('Exists:', p.exists())
if not p.exists():
    raise SystemExit(1)

conn = sqlite3.connect(str(p))
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
rows = [r[0] for r in cur.fetchall()]
print('Tables:')
for r in rows:
    print(' ', r)

cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE 'turbine_%'")
print('turbine_tables_count:', cur.fetchone()[0])

# counts per turbine table
for t in rows:
    if t.startswith('turbine_'):
        try:
            cur.execute(f"SELECT COUNT(*) FROM \"{t}\"")
            c = cur.fetchone()[0]
        except Exception as e:
            c = f'ERR: {e}'
        print(f'  {t}: {c}')

conn.close()
print('Done')

