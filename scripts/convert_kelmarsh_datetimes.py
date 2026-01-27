#!/usr/bin/env python3
"""Normalize "Date and time" values in kelmarsh per-turbine DB tables 2..6.

Goal: Convert entries like 2016-06-08T00:00:00+0000 (ISO with timezone)
into format like turbine_1: 2016-06-08 00:00:00 (space, no timezone).

The script will:
 - for tables turbine_2..turbine_6, select rows where "Date and time" contains 'T' or 'Z' or '+'
 - parse the timestamp, normalize to UTC and format as 'YYYY-MM-DD HH:MM:SS'
 - update the row if the new formatted value differs from the stored one
 - log counts and a few before/after samples

No backups are created here (per your request). Use with care.
"""
import sqlite3
from pathlib import Path
import re
import datetime
import sys
import time

DB = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\kelmarsh_data_by_turbine.db")
TABLES = [f"turbine_{i}" for i in range(2, 7)]
COL = 'Date and time'
BATCH = 5000
RETRY_SLEEP = 1.0
MAX_RETRIES = 5

if not DB.exists():
    print('DB not found:', DB)
    sys.exit(2)

print('DB:', DB)
print('Tables to normalize:', TABLES)
print('Batch size:', BATCH)

# helper to normalize an ISO-like timestamp string to 'YYYY-MM-DD HH:MM:SS'
# returns None if unable to parse

def normalize_ts(s):
    if s is None:
        return None
    s = s.strip()
    if not s:
        return s
    # quick check: if already in desired format (space, no T), and length matches, leave
    # Accept patterns like 'YYYY-MM-DD HH:MM:SS' (19 chars)
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", s):
        return s
    # Try to convert ISO-like forms
    # Common forms: 2016-06-08T00:00:00+0000 or 2016-06-08T00:00:00+00:00 or 2016-06-08T00:00:00Z
    t = s
    # If ends with 'Z', replace with +00:00
    if t.endswith('Z'):
        t = t[:-1] + '+00:00'
    # If timezone is in form +0000 or -0000 (no colon), convert to +00:00
    m = re.search(r'([+-])(\d{2})(\d{2})$', t)
    if m:
        sign, hh, mm = m.groups()
        tz = f"{sign}{hh}:{mm}"
        t = re.sub(r'([+-])\d{4}$', tz, t)
    # If there's a T separating date/time, ensure it's OK for fromisoformat
    try:
        # datetime.fromisoformat supports offsets like +00:00
        dt = datetime.datetime.fromisoformat(t)
        # Convert to UTC if tz-aware
        if dt.tzinfo is not None:
            dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        # Format to desired string
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        pass
    # Fallback: try common strptime patterns
    patterns = [
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S%z',
        '%Y-%m-%d %H:%M:%S',
    ]
    for p in patterns:
        try:
            dt = datetime.datetime.strptime(s, p)
            # if dt has tzinfo, convert to UTC naive
            if dt.tzinfo is not None:
                dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            continue
    # As last resort, try to strip timezone part (+xxx) and replace T with space
    t2 = re.sub(r'[T]', ' ', s)
    t2 = re.sub(r'\+\d{4}$', '', t2)
    t2 = re.sub(r'Z$', '', t2)
    t2 = t2.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", t2):
        return t2
    return None

# Open DB
conn = sqlite3.connect(str(DB), timeout=30.0)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

for tbl in TABLES:
    print('\nProcessing', tbl)
    # check table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
    if not cur.fetchone():
        print('  Table not found, skipping')
        continue
    changed = 0
    samples = []
    offset = 0
    # batch loop: select LIMIT rows matching heuristic
    while True:
        retries = 0
        rows = None
        while retries <= MAX_RETRIES:
            try:
                cur.execute(
                    f"SELECT rowid, \"{COL}\" as dt FROM '{tbl}' WHERE (\"{COL}\" LIKE '%T%' OR \"{COL}\" LIKE '%Z' OR \"{COL}\" LIKE '%+%') LIMIT ? OFFSET ?",
                    (BATCH, offset),
                )
                rows = cur.fetchall()
                break
            except sqlite3.OperationalError as e:
                retries += 1
                print(f'  SELECT failed (attempt {retries})', e)
                time.sleep(RETRY_SLEEP * retries)
        if rows is None:
            print('  Failed to fetch batch; aborting table')
            break
        if not rows:
            # no more candidate rows at offsets; reset offset and try without offset to catch new ones
            if offset == 0:
                break
            else:
                offset = 0
                # do one more loop to ensure no remaining
                continue
        # process rows
        for r in rows:
            rid = r['rowid']
            val = r['dt']
            norm = normalize_ts(val)
            if norm and norm != val:
                update_retries = 0
                while update_retries <= MAX_RETRIES:
                    try:
                        cur.execute(f"UPDATE '{tbl}' SET \"{COL}\" = ? WHERE rowid = ?", (norm, rid))
                        changed += 1
                        if len(samples) < 5:
                            samples.append((val, norm))
                        break
                    except sqlite3.OperationalError as e:
                        update_retries += 1
                        print(f'  UPDATE failed for row {rid} (attempt {update_retries})', e)
                        time.sleep(RETRY_SLEEP * update_retries)
                    except Exception as e:
                        print('  ERROR updating row', rid, e)
                        break
        # commit after batch
        try:
            conn.commit()
        except sqlite3.OperationalError as e:
            print('  COMMIT failed:', e, '; retrying')
            time.sleep(RETRY_SLEEP)
            try:
                conn.commit()
            except Exception as e2:
                print('  COMMIT retry failed, aborting table:', e2)
                break
        print(f'  Processed batch offset {offset}, rows: {len(rows)}, changes so far: {changed}')
        # advance offset
        offset += BATCH
    print('  Changes applied:', changed)
    if samples:
        print('  Samples (before -> after):')
        for a, b in samples:
            print('   ', a, '->', b)

cur.close()
conn.close()
print('\nDone.')

