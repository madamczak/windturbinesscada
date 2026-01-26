"""Merge kelmarsh_1..6 sqlite DBs into single DB with turbine column.

Behavior:
- Look for databases data/sqlitedbs/kelmarsh_{i}_data.db for i=1..6
- For each existing DB, pick the first non-sqlite_sequence table and collect its columns
- Compute union of all columns
- Create output DB data/sqlitedbs/kelmarsh_1_6_all.db with table `kelmarsh_all` having columns:
    - turbine INTEGER
    - all union columns as TEXT (to be permissive)
- Copy rows from each source table to the merged table, mapping columns; missing columns -> NULL
- Use transactions and chunked SELECT to avoid high memory use

Usage: python scripts/merge_kelmarsh_1_6.py
"""
from __future__ import annotations
import os
import sqlite3
from typing import List, Dict

SQLITEDIR = os.path.join('data', 'sqlitedbs')
OUT_DB = os.path.join(SQLITEDIR, 'kelmarsh_1_6_all.db')
OUT_TABLE = 'kelmarsh_all'


def get_tables_and_columns(db_path: str) -> Dict[str, List[str]]:
    if not os.path.exists(db_path):
        return {}
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    tables = [t for t in tables if t != 'sqlite_sequence']
    res = {}
    for t in tables:
        cols = [r[1] for r in cur.execute(f'PRAGMA table_info("{t}")').fetchall()]
        res[t] = cols
    conn.close()
    return res


def collect_union_columns() -> Dict[int, Dict[str, List[str]]]:
    """Return mapping turbine -> {table: cols} and also build union set."""
    mapping = {}
    for i in range(1, 7):
        db = os.path.join(SQLITEDIR, f'kelmarsh_{i}_data.db')
        tables = get_tables_and_columns(db)
        mapping[i] = tables
    return mapping


def create_output_db(columns: List[str]):
    # drop existing
    if os.path.exists(OUT_DB):
        os.remove(OUT_DB)
    conn = sqlite3.connect(OUT_DB)
    cur = conn.cursor()
    # create table with turbine INTEGER first, then all columns as TEXT
    col_defs = [f'"{c}" TEXT' for c in columns]
    sql = f'CREATE TABLE {OUT_TABLE} (turbine INTEGER, ' + ', '.join(col_defs) + ')'
    cur.execute(sql)
    conn.commit()
    conn.close()


def insert_rows_from_source(src_db: str, src_table: str, src_cols: List[str], turbine:int):
    conn_src = sqlite3.connect(src_db)
    cur_src = conn_src.cursor()
    conn_out = sqlite3.connect(OUT_DB)
    cur_out = conn_out.cursor()
    # Get union columns from output table
    out_cols = [r[1] for r in cur_out.execute(f'PRAGMA table_info("{OUT_TABLE}")').fetchall() if r[1] != 'turbine']

    # Build select projection from src_cols to match out_cols order
    select_cols = []
    for c in out_cols:
        if c in src_cols:
            select_cols.append(f'"{c}"')
        else:
            select_cols.append('NULL')
    select_sql = f'SELECT {", ".join(select_cols)} FROM "{src_table}"'

    batch = 10000
    cur_src.execute(select_sql)
    rows_fetched = 0
    insert_placeholders = ','.join(['?'] * (1 + len(out_cols)))
    insert_sql = f'INSERT INTO {OUT_TABLE} (turbine, ' + ', '.join([f'"{c}"' for c in out_cols]) + f') VALUES ({insert_placeholders})'
    while True:
        rows = cur_src.fetchmany(batch)
        if not rows:
            break
        to_insert = []
        for r in rows:
            to_insert.append((turbine, ) + tuple(r))
        cur_out.executemany(insert_sql, to_insert)
        conn_out.commit()
        rows_fetched += len(rows)
    conn_src.close()
    conn_out.close()
    return rows_fetched


def main():
    mapping = collect_union_columns()
    # Build union of column names
    union_cols = []
    seen = set()
    for i in range(1,7):
        tables = mapping.get(i, {})
        if not tables:
            continue
        # pick first table
        tlist = list(tables.items())
        if not tlist:
            continue
        _, cols = tlist[0]
        for c in cols:
            if c not in seen:
                seen.add(c)
                union_cols.append(c)
    if not union_cols:
        print('No columns found in any source DBs. Nothing to do.')
        return
    print(f'Creating output DB {OUT_DB} with {len(union_cols)} columns (plus turbine)')
    create_output_db(union_cols)

    total_rows = 0
    for i in range(1,7):
        db = os.path.join(SQLITEDIR, f'kelmarsh_{i}_data.db')
        if not os.path.exists(db):
            print('Source missing:', db)
            continue
        tables = mapping.get(i, {})
        if not tables:
            print('No tables in', db)
            continue
        src_table = list(tables.keys())[0]
        src_cols = tables[src_table]
        print(f'Processing turbine {i} from {db} table {src_table} cols={len(src_cols)}')
        n = insert_rows_from_source(db, src_table, src_cols, turbine=i)
        print('  rows copied:', n)
        total_rows += n
    print('Done. Total rows copied:', total_rows)


if __name__ == '__main__':
    main()

