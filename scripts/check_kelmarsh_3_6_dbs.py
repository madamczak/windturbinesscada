import os
import sqlite3

for t in range(3,7):
    db = os.path.join('data','sqlitedbs', f'kelmarsh_{t}_data.db')
    print('\nDB:', db)
    if not os.path.exists(db):
        print('  MISSING')
        continue
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    tables = [t for t in tables if t!='sqlite_sequence']
    print('  tables:', tables)
    for tab in tables:
        cnt = cur.execute(f'SELECT COUNT(*) FROM "{tab}"').fetchone()[0]
        print('   -', tab, 'rows=', cnt)
    conn.close()

print('\nDone')
