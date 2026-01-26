import sqlite3
import os
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB = os.path.join(BASE_DIR, 'data', 'sqlitedbs', 'penmanshiel_all_data.db')
if not os.path.exists(DB):
    print('DB not found:', DB)
else:
    conn = sqlite3.connect(DB)
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables = [r[0] for r in cur.fetchall()]
    print('Tables:', tables)
    for t in tables:
        print('\nSchema for', t)
        for row in conn.execute(f"PRAGMA table_info('{t}')"):
            print(row)
        print('\nCounts per Turbine:')
        for row in conn.execute(f"SELECT Turbine, COUNT(*) FROM '{t}' GROUP BY Turbine ORDER BY Turbine"):
            print(row)
    conn.close()

