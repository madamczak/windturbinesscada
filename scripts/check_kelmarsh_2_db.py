import os
import sqlite3
import sys

DB_PATH = os.path.join('data', 'sqlitedbs', 'kelmarsh_2_data.db')
print('DB_PATH', DB_PATH)
print('EXISTS', os.path.exists(DB_PATH))
if not os.path.exists(DB_PATH):
    sys.exit(1)
print('SIZE', os.path.getsize(DB_PATH))

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
try:
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
except Exception as e:
    print('ERROR_LIST_TABLES', e)
    tables = []
print('TABLES', tables)

for t in tables:
    try:
        cnt = cur.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
    except Exception as e:
        cnt = f'ERROR: {e}'
    print('COUNT', t, cnt)

conn.close()

