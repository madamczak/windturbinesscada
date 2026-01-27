#!/usr/bin/env python3
"""Safely delete rows before cutoff 2016-06-07 00:00:00 in penmanshiel per-turbine DB.

This script uses WAL journal mode, a busy timeout, and deletes in batches by rowid
so each transaction is short and is less likely to encounter "database is locked".

No backups are created (per user request). Use with care.
"""
import sqlite3
from pathlib import Path
import time

DB = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\penmanshiel_data_by_turbine.db")
CUTOFF = '2016-06-07 00:00:00'
BATCH = 5000
BUSY_TIMEOUT = 30.0  # seconds

if not DB.exists():
    print('DB not found:', DB)
    raise SystemExit(2)

print('DB:', DB)
print('Cutoff:', CUTOFF)
print('Batch size:', BATCH)

# helper to get a new connection with pragmas
def make_conn():
    conn = sqlite3.connect(str(DB), timeout=BUSY_TIMEOUT, isolation_level=None)
    cur = conn.cursor()
    try:
        cur.execute('PRAGMA journal_mode = WAL;')
    except Exception:
        pass
    try:
        cur.execute('PRAGMA synchronous = NORMAL;')
    except Exception:
        pass
    return conn

summary = []
for t in range(1, 16):
    tbl = f"turbine_{t}"
    print('\nTable:', tbl)
    conn = make_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
        if not cur.fetchone():
            print('  Table not found, skipping')
            summary.append((tbl, 'missing', 0))
            cur.close(); conn.close();
            continue
        # count matching rows before delete
        cur.execute(f"SELECT COUNT(*) FROM '{tbl}' WHERE \"Date and time\" < ?", (CUTOFF,))
        before = cur.fetchone()[0]
        print('  Matching rows (before):', before)
        if before <= 0:
            summary.append((tbl, 'none', 0))
            cur.close(); conn.close();
            continue
        total_deleted = 0
        attempt = 0
        while True:
            attempt += 1
            # select a batch of rowids matching the condition
            try:
                cur.execute(f"SELECT rowid FROM '{tbl}' WHERE \"Date and time\" < ? LIMIT ?", (CUTOFF, BATCH))
                rows = cur.fetchall()
            except sqlite3.OperationalError as e:
                print('  SELECT rowid failed:', e, '; retrying after backoff')
                time.sleep(1 + attempt)
                continue
            if not rows:
                break
            ids = [r[0] for r in rows]
            # delete by rowid in a short transaction
            placeholders = ','.join('?' for _ in ids)
            sql = f"DELETE FROM '{tbl}' WHERE rowid IN ({placeholders})"
            try:
                cur.execute('BEGIN')
                cur.execute(sql, ids)
                cur.execute('COMMIT')
                deleted = cur.rowcount
                total_deleted += deleted
                print(f'  Batch deleted: {deleted} (total {total_deleted})')
            except sqlite3.OperationalError as e:
                print('  DELETE batch failed:', e, '; rolling back and retrying after backoff')
                try:
                    cur.execute('ROLLBACK')
                except Exception:
                    pass
                time.sleep(1 + attempt)
                continue
        # verify none remain
        cur.execute(f"SELECT COUNT(*) FROM '{tbl}' WHERE \"Date and time\" < ?", (CUTOFF,))
        after = cur.fetchone()[0]
        print('  Matching rows (after):', after)
        summary.append((tbl, 'deleted', total_deleted))
    except Exception as e:
        print('  ERROR processing table:', e)
        summary.append((tbl, 'error', 0))
    finally:
        try: cur.close()
        except: pass
        try: conn.close()
        except: pass

print('\nSummary:')
for s in summary:
    print(' ', s)
print('\nDone.')

