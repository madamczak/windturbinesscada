#!/usr/bin/env python3
"""Delete rows before a given cutoff in kelmarsh per-turbine DB and print counts.

Deletes without creating a backup (per user request). Use with care.
"""
import sqlite3
from pathlib import Path

DB = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\kelmarsh_data_by_turbine.db")
CUTOFF = '2016-06-06 18:10:00'

if not DB.exists():
    print('DB not found:', DB)
    raise SystemExit(2)

conn = sqlite3.connect(str(DB))
cur = conn.cursor()

print('DB:', DB)
print('Cutoff:', CUTOFF)

summary = []
for t in range(1, 7):
    tbl = f"turbine_{t}"
    print('\nTable:', tbl)
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
        if not cur.fetchone():
            print('  Table not found, skipping')
            summary.append((tbl, 'missing', 0))
            continue
        # count matching rows before delete
        try:
            cur.execute(f"SELECT COUNT(*) FROM '{tbl}' WHERE \"Date and time\" < ?", (CUTOFF,))
            before = cur.fetchone()[0]
        except Exception as e:
            print('  Error counting by "Date and time":', e)
            cur.execute(f"SELECT COUNT(*) FROM '{tbl}'")
            total = cur.fetchone()[0]
            print('  Total rows in table (no delete performed):', total)
            summary.append((tbl, 'error_count', total))
            continue
        print('  Matching rows (before):', before)
        if before > 0:
            try:
                cur.execute(f"DELETE FROM '{tbl}' WHERE \"Date and time\" < ?", (CUTOFF,))
                conn.commit()
                deleted = cur.rowcount
                print('  Deleted rows reported by cursor:', deleted)
            except Exception as e:
                print('  ERROR deleting rows:', e)
                conn.rollback()
                summary.append((tbl, 'error_delete', 0))
                continue
            # verify none remain
            cur.execute(f"SELECT COUNT(*) FROM '{tbl}' WHERE \"Date and time\" < ?", (CUTOFF,))
            after = cur.fetchone()[0]
            print('  Matching rows (after):', after)
            summary.append((tbl, 'deleted', deleted))
        else:
            print('  Nothing to delete')
            summary.append((tbl, 'none', 0))
    except Exception as e:
        print('  ERROR processing table:', e)
        summary.append((tbl, 'error', 0))

cur.close()
conn.close()

print('\nSummary:')
for s in summary:
    print(' ', s)
print('\nDone.')

