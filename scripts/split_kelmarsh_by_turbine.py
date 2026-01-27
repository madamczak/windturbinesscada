#!/usr/bin/env python3
"""Split kelmarsh_all_data.db into a new sqlite DB with one table per turbine.

Strategy:
- Read the first user table from the source DB.
- Inspect column names and build heuristics mapping columns to turbine numbers (1..max_turbines).
- Iterate rows in source table and, for each row, decide which turbine(s) it belongs to by checking:
  - If any turbine-specific candidate column for turbine N is non-empty for that row
  - Or if there is a column named like 'turbine' or 'turbine_id' whose value equals N
- Create output DB (kelmarsh_by_turbine.db) and create tables turbine_1 ... turbine_N with the same column schema.
- Insert rows into the corresponding turbine tables (include original_rowid as src_rowid).

This is conservative and logs progress; you can adjust MAX_TURBINES if needed.
"""

import sqlite3
from pathlib import Path
import re
import sys

SRC = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\kelmarsh_all_data.db")
OUT = SRC.parent / "kelmarsh_by_turbine.db"
MAX_TURBINES = 6  # adjust if needed
BATCH = 500

if not SRC.exists():
    print("Source DB not found:", SRC)
    sys.exit(1)

print('Source DB:', SRC)
if OUT.exists():
    print('Removing existing output DB:', OUT)
    OUT.unlink()

sconn = sqlite3.connect(str(SRC))
sconn.row_factory = sqlite3.Row
scur = sconn.cursor()

# find first user table
scur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 1")
t = scur.fetchone()
if not t:
    print('No table found in source DB')
    sys.exit(1)

table = t[0]
print('Using source table:', table)

# get columns and types
qtable = table.replace("'", "''")
scur.execute(f"PRAGMA table_info('{qtable}')")
cols_info = scur.fetchall()
cols = [r[1] for r in cols_info]
col_types = {r[1]: r[2] for r in cols_info}
print('Columns:', cols)

# build candidate column lists per turbine
cand_by_turb = {n: [] for n in range(1, MAX_TURBINES+1)}
# Prefer an exact 'turbine' column (case-insensitive) if present
turbine_id_col = next((c for c in cols if c.lower() == 'turbine'), None)
if turbine_id_col is None:
    # try a few common alternate exact names
    for alt in ('turbine_id', 'turb_id', 'turb_no', 'turbine_no'):
        turbine_id_col = next((c for c in cols if c.lower() == alt), None)
        if turbine_id_col:
            break

for c in cols:
    lc = c.lower()
    # explicit preferred id column names (higher priority)
    if re.search(r"\bturbine\b|\bturbine_id\b|\bturb_id\b|\bturb_no\b|\bturbine_no\b|\bturb\b|\bwt\b", lc):
        # if the column name contains a digit (e.g., turb1_value) treat as candidate per-turbine
        if re.search(r"(\d+)", lc):
            for m in re.finditer(r"(\d+)", lc):
                num = int(m.group(1))
                if 1 <= num <= MAX_TURBINES:
                    cand_by_turb[num].append(c)
        else:
            # if we haven't already found a clear turbine id column, prefer exact or close matches
            if turbine_id_col is None:
                if re.fullmatch(r"turbine" , lc) or re.fullmatch(r"turbine_id", lc) or re.fullmatch(r"turb_id", lc):
                    turbine_id_col = c
                elif 'turbine' in lc:
                    turbine_id_col = c

    # patterns like suffix _1 or .1 or -1 or isolated digit near word boundaries
    for n in range(1, MAX_TURBINES+1):
        if re.search(rf"(^|[^0-9]){n}($|[^0-9])", lc) and ("turb" in lc or re.search(r"[_.\-]", lc) or lc.startswith('wt')):
            cand_by_turb[n].append(c)
        # patterns like t1, wt1
        if re.search(rf"t\s*{n}", lc) or re.search(rf"wt\s*{n}", lc):
            cand_by_turb[n].append(c)

# dedupe
for n in cand_by_turb:
    cand_by_turb[n] = sorted(set(cand_by_turb[n]))

print('Candidate columns per turbine:')
for n in range(1, MAX_TURBINES+1):
    print(f'  Turbine {n}:', cand_by_turb[n])
print('Detected turbine id column:', turbine_id_col)

# prepare output DB and create per-turbine tables
oconn = sqlite3.connect(str(OUT))
ocur = oconn.cursor()

# create table schema string (excluding leading rowid because we'll include src_rowid)
# reuse columns and types; make everything TEXT to be permissive
col_defs = []
for c in cols:
    # use declared type if available, else TEXT
    ttype = col_types.get(c, '') or 'TEXT'
    # sanitize column name to be quoted
    col_defs.append(f'"{c}" {ttype}')
# add src_rowid to keep reference
schema_cols = ', '.join(['src_rowid INTEGER'] + col_defs)

for n in range(1, MAX_TURBINES+1):
    tname = f'turbine_{n}'
    ocur.execute(f'CREATE TABLE "{tname}" ({schema_cols})')
    print('Created table', tname)

oconn.commit()

# iterate rows and distribute
scur.execute(f"SELECT rowid, * FROM '{qtable}'")
count = 0
insert_counts = {n: 0 for n in range(1, MAX_TURBINES+1)}
rows_buffer = {n: [] for n in range(1, MAX_TURBINES+1)}

for row in scur:
    count += 1
    # build dict col->value
    rowdict = {c: row[c] for c in cols}
    src_rowid = row[0]
    assigned = set()
    # If a dedicated turbine id column was detected, use it strictly
    if turbine_id_col:
        try:
            v = rowdict.get(turbine_id_col)
            if v is not None:
                sv = str(v).strip()
                m = re.search(r"(\d+)", sv)
                if m:
                    num = int(m.group(1))
                    if 1 <= num <= MAX_TURBINES:
                        assigned.add(num)
        except Exception:
            pass
    else:
        # check candidate columns per turbine
        for n in range(1, MAX_TURBINES+1):
            for c in cand_by_turb.get(n, []):
                v = rowdict.get(c)
                if v is None:
                    continue
                if isinstance(v, (int, float)):
                    try:
                        if float(v) != 0:
                            assigned.add(n)
                            break
                    except Exception:
                        pass
                sval = str(v).strip()
                if sval != '' and sval.lower() not in ('none', 'nan'):
                    assigned.add(n)
                    break
    # if still unassigned, you may choose to put in turbine_1 by default; we will skip
    if not assigned:
        # skip rows that do not map to any turbine to keep tables per turbine clean
        continue
    # insert into each assigned turbine table
    for n in assigned:
        tname = f'turbine_{n}'
        # prepare values in same order as schema_cols (src_rowid then original cols)
        vals = [src_rowid] + [rowdict.get(c) for c in cols]
        rows_buffer[n].append(vals)
        insert_counts[n] += 1

    # periodically flush
    if count % BATCH == 0:
        for n in range(1, MAX_TURBINES+1):
            if rows_buffer[n]:
                placeholders = ','.join(['?'] * (1 + len(cols)))
                ocur.executemany(f'INSERT INTO "turbine_{n}" VALUES ({placeholders})', rows_buffer[n])
                rows_buffer[n].clear()
        oconn.commit()
        print(f'Processed {count} rows...')

# final flush
for n in range(1, MAX_TURBINES+1):
    if rows_buffer[n]:
        placeholders = ','.join(['?'] * (1 + len(cols)))
        ocur.executemany(f'INSERT INTO "turbine_{n}" VALUES ({placeholders})', rows_buffer[n])
        rows_buffer[n].clear()
oconn.commit()

print('Done. Processed rows:', count)
print('Insert counts per turbine:')
for n in range(1, MAX_TURBINES+1):
    print(f'  Turbine {n}: {insert_counts[n]}')

sconn.close()
oconn.close()
print('Output DB:', OUT)
