#!/usr/bin/env python3
"""Convert "Date and time" values that use 'T' or timezone forms to 'YYYY-MM-DD HH:MM:SS'
for turbine_2 .. turbine_6 in kelmarsh_data_by_turbine.db using SQL UPDATE statements.

The script logs counts before/after and a few sample rows for verification.
"""
import sqlite3
from pathlib import Path

DB = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\kelmarsh_data_by_turbine.db")
TABLES = [f"turbine_{i}" for i in range(2, 7)]
COL = 'Date and time'

if not DB.exists():
    print('DB not found:', DB)
    raise SystemExit(2)

conn = sqlite3.connect(str(DB))
cur = conn.cursor()

for tbl in TABLES:
    print('\nTable:', tbl)
    # count candidate rows containing 'T' (ISO style) or 'Z' (UTC marker) or a '+' timezone
    try:
        cur.execute(f"SELECT COUNT(*) FROM '{tbl}' WHERE \"{COL}\" LIKE '%T%' OR \"{COL}\" LIKE '%Z' OR \"{COL}\" LIKE '%+%'")
        before = cur.fetchone()[0]
    except Exception as e:
        print('  ERROR counting candidates:', e)
        continue
    print('  Candidates to convert (before):', before)
    if before <= 0:
        print('  Nothing to do')
        continue
    # show up to 5 sample rows before conversion
    try:
        cur.execute(f"SELECT rowid, \"{COL}\" FROM '{tbl}' WHERE \"{COL}\" LIKE '%T%' OR \"{COL}\" LIKE '%Z' OR \"{COL}\" LIKE '%+%' LIMIT 5")
        samples_before = cur.fetchall()
        print('  Samples before:')
        for r in samples_before:
            print('   ', r)
    except Exception as e:
        print('  (could not fetch samples before)', e)
    # Run UPDATE: take first 19 chars (YYYY-MM-DDTHH:MM:SS) then replace 'T' with space
    try:
        sql = f"UPDATE '{tbl}' SET \"{COL}\" = replace(substr(\"{COL}\",1,19), 'T', ' ') WHERE \"{COL}\" LIKE '%T%' OR \"{COL}\" LIKE '%Z' OR \"{COL}\" LIKE '%+%';"
        cur.execute('BEGIN')
        cur.execute(sql)
        affected = cur.rowcount
        conn.commit()
        print('  UPDATE executed, cursor.rowcount:', affected)
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print('  ERROR executing UPDATE:', e)
        continue
    # count remaining candidates
    try:
        cur.execute(f"SELECT COUNT(*) FROM '{tbl}' WHERE \"{COL}\" LIKE '%T%' OR \"{COL}\" LIKE '%Z' OR \"{COL}\" LIKE '%+%'")
        after = cur.fetchone()[0]
    except Exception as e:
        print('  ERROR counting after:', e)
        after = None
    print('  Candidates remaining (after):', after)
    # show up to 5 samples after
    try:
        cur.execute(f"SELECT rowid, \"{COL}\" FROM '{tbl}' LIMIT 5")
        samples_after = cur.fetchall()
        print('  Samples after:')
        for r in samples_after:
            print('   ', r)
    except Exception as e:
        print('  (could not fetch samples after)', e)

cur.close()
conn.close()
print('\nDone.')

