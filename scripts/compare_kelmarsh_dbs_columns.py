import os
import sqlite3
import json

DB1 = os.path.join('data', 'sqlitedbs', 'kelmarsh_1_data.db')
DB2 = os.path.join('data', 'sqlitedbs', 'kelmarsh_2_data.db')

def get_cols(db):
    if not os.path.exists(db):
        return None
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    tables = [t for t in tables if t != 'sqlite_sequence']
    cols = {}
    for t in tables:
        cols[t] = [r[1] for r in cur.execute(f'PRAGMA table_info("{t}")').fetchall()]
    conn.close()
    return cols

c1 = get_cols(DB1)
c2 = get_cols(DB2)

out = {'db1_path': DB1, 'db2_path': DB2}
if c1 is None:
    out['error_db1'] = 'missing'
if c2 is None:
    out['error_db2'] = 'missing'
if c1 and c2:
    # For comparison, pick the first table from each DB
    t1 = next(iter(c1.keys()))
    t2 = next(iter(c2.keys()))
    cols1 = c1[t1]
    cols2 = c2[t2]
    set1 = set(cols1)
    set2 = set(cols2)
    only1 = sorted(list(set1 - set2))
    only2 = sorted(list(set2 - set1))
    common = sorted(list(set1 & set2))
    out.update({
        'table_db1': t1,
        'table_db2': t2,
        'cols_count_db1': len(cols1),
        'cols_count_db2': len(cols2),
        'only_in_db1_count': len(only1),
        'only_in_db2_count': len(only2),
        'only_in_db1_sample': only1[:50],
        'only_in_db2_sample': only2[:50],
        'common_count': len(common),
        'common_sample': common[:50]
    })

print(json.dumps(out, ensure_ascii=False, indent=2))

