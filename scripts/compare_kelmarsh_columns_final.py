import os
import sqlite3

DB1 = os.path.join('data', 'sqlitedbs', 'kelmarsh_1_data.db')
DB2 = os.path.join('data', 'sqlitedbs', 'kelmarsh_2_data.db')

def get_first_table_cols(db_path):
    if not os.path.exists(db_path):
        return None, []
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    tables = [t for t in tables if t != 'sqlite_sequence']
    if not tables:
        conn.close()
        return None, []
    t = tables[0]
    cols = [r[1] for r in cur.execute(f'PRAGMA table_info("{t}")').fetchall()]
    conn.close()
    return t, cols

t1, cols1 = get_first_table_cols(DB1)
t2, cols2 = get_first_table_cols(DB2)

set1 = set(cols1)
set2 = set(cols2)
only1 = sorted(list(set1 - set2))
only2 = sorted(list(set2 - set1))
common = sorted(list(set1 & set2))

print(f"DB1: {DB1} table: {t1} cols: {len(cols1)}")
print(f"DB2: {DB2} table: {t2} cols: {len(cols2)}")
print()
print(f"Only in DB1 ({len(only1)}):")
for c in only1[:200]:
    print(c)
if len(only1) > 200:
    print('... and', len(only1)-200, 'more')

print()
print(f"Only in DB2 ({len(only2)}):")
for c in only2[:200]:
    print(c)
if len(only2) > 200:
    print('... and', len(only2)-200, 'more')

print()
print(f"Common columns: {len(common)} (showing first 50):")
for c in common[:50]:
    print(c)

