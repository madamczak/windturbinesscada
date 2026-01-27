import sqlite3
from pathlib import Path

DB = Path('data/sqlitedbs/penmanshiel_all_data.db')
if not DB.exists():
    print('DB not found:', DB)
    raise SystemExit(1)

conn = sqlite3.connect(str(DB))
cur = conn.cursor()
print('Connected to', DB)

# Print counts for turbine_1..turbine_15 and turbine_unknown
for i in range(1, 16):
    tbl = f'turbine_{i}'
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{tbl}"')
        cnt = cur.fetchone()[0]
    except Exception as e:
        cnt = f'ERR: {e}'
    print(f'{tbl}: {cnt}')

try:
    cur.execute('SELECT COUNT(*) FROM "turbine_unknown"')
    unk = cur.fetchone()[0]
except Exception as e:
    unk = f'ERR: {e}'
print('turbine_unknown:', unk)

# Distinct Turbine values in turbine_1
try:
    cur.execute('SELECT DISTINCT(TRIM(Turbine)) FROM turbine_1 ORDER BY 1;')
    vals = [r[0] for r in cur.fetchall()]
    print('Distinct Turbine values in turbine_1:', vals[:50])
except Exception as e:
    print('Could not query turbine_1 distinct Turbine:', e)

# Original combined table row count
try:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_all%';")
    tbl = cur.fetchone()
    if tbl:
        combined = tbl[0]
        cur.execute(f'SELECT COUNT(*) FROM "{combined}"')
        total = cur.fetchone()[0]
        print('Combined table:', combined, 'rows:', total)
    else:
        print('No combined _all table found')
except Exception as e:
    print('Error checking combined table:', e)

conn.close()
print('Done')

