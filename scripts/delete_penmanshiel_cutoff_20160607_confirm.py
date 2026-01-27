#!/usr/bin/env python3
"""Confirm and delete rows before cutoff 2016-06-07 00:00:00 in penmanshiel DB.

For each turbine table this script:
 - counts rows with "Date and time" < cutoff
 - if any, attempts DELETE with retries on lock
 - counts after and reports deleted = before - after
"""
import sqlite3
from pathlib import Path
import time

DB = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\penmanshiel_data_by_turbine.db")
CUTOFF = '2016-06-07 00:00:00'
RETRIES = 8
RETRY_SLEEP = 1.0
TIMEOUT = 30.0

if not DB.exists():
    print('DB not found:', DB); raise SystemExit(2)

print('DB:', DB)
print('Cutoff:', CUTOFF)

def make_conn():
    conn = sqlite3.connect(str(DB), timeout=TIMEOUT)
    cur = conn.cursor()
    try:
        cur.execute('PRAGMA journal_mode = WAL;')
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
        cur.execute(f"SELECT COUNT(*) FROM '{tbl}' WHERE \"Date and time\" < ?", (CUTOFF,))
        before = cur.fetchone()[0]
        print('  Matching rows (before):', before)
        if before <= 0:
            summary.append((tbl, 'none', 0))
            cur.close(); conn.close();
            continue
        # attempt delete with retries
        deleted = None
        for attempt in range(1, RETRIES+1):
            try:
                cur.execute(f"DELETE FROM '{tbl}' WHERE \"Date and time\" < ?", (CUTOFF,))
                conn.commit()
                # compute after
                cur.execute(f"SELECT COUNT(*) FROM '{tbl}' WHERE \"Date and time\" < ?", (CUTOFF,))
                after = cur.fetchone()[0]
                deleted = before - after
                print('  Deleted (computed):', deleted)
                summary.append((tbl, 'deleted', deleted))
                break
            except sqlite3.OperationalError as e:
                print(f'  Attempt {attempt} - OperationalError:', e)
                time.sleep(RETRY_SLEEP * attempt)
                continue
            except Exception as e:
                print('  DELETE error:', e)
                break
        if deleted is None:
            print('  Failed to delete after retries')
            summary.append((tbl, 'failed', 0))
    finally:
        try: cur.close()
        except: pass
        try: conn.close()
        except: pass

print('\nSummary:')
for s in summary:
    print(' ', s)
print('\nDone.')

