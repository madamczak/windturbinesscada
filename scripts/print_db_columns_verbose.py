import os
import sqlite3

DB_PATHS = [
    os.path.join('data', 'sqlitedbs', 'kelmarsh_1_data.db'),
    os.path.join('data', 'sqlitedbs', 'kelmarsh_2_data.db'),
]

for db in DB_PATHS:
    print('DB:', db)
    if not os.path.exists(db):
        print('  MISSING')
        continue
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    tables = [t for t in tables if t != 'sqlite_sequence']
    if not tables:
        print('  (no tables)')
        conn.close()
        continue
    for t in tables:
        print(' TABLE:', t)
        cols = cur.execute(f'PRAGMA table_info("{t}")').fetchall()
        for c in cols:
            print('   -', c[1])
    conn.close()
    print()

