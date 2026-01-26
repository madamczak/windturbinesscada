"""
Memory-efficient merge of per-turbine Penmanshiel SQLite DBs into a single DB.
Scans for files: data/sqlitedbs/penmanshiel_XX_data.db where XX=01..15
Creates output: data/sqlitedbs/penmanshiel_all_data.db with table "Penmanshiel Data" and an extra column "Turbine" (text, two-digit string).

This script uses SQLite ATTACH + SQL to copy rows without loading into Python memory.
"""
import os
import sqlite3

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SQLITE_DIR = os.path.join(BASE_DIR, 'data', 'sqlitedbs')
OUT_DB = os.path.join(SQLITE_DIR, 'penmanshiel_all_data.db')


def find_db_for_turbine(n):
    fname = f'penmanshiel_{n:02d}_data.db'
    return os.path.join(SQLITE_DIR, fname)


def detect_table_in_conn(conn, schema_alias=''):
    # schema_alias: if attached, pass like 'src.', else '' for main
    cur = conn.execute(f"SELECT name FROM {schema_alias}sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables = [r[0] for r in cur.fetchall()]
    if not tables:
        return None
    # prefer names that contain 'Penmanshiel' or 'Data'
    for t in tables:
        if 'penmanshiel' in t.lower() and 'data' in t.lower():
            return t
    for t in tables:
        if 'data' in t.lower():
            return t
    # fallback to first
    return tables[0]


def merge():
    # remove existing output
    if os.path.exists(OUT_DB):
        print('Removing existing output DB:', OUT_DB)
        os.remove(OUT_DB)

    out_conn = sqlite3.connect(OUT_DB)
    out_conn.execute('PRAGMA journal_mode=wal;')
    out_conn.commit()

    created = False
    processed = []

    try:
        for n in range(1, 16):
            db_path = find_db_for_turbine(n)
            if not os.path.exists(db_path):
                print(f'No DB for turbine {n:02d}: {db_path} (skipping)')
                continue
            alias = f'src{n}'
            print(f'Attaching {db_path} as {alias}...')
            out_conn.execute(f"ATTACH DATABASE ? AS {alias}", (db_path,))
            # detect table in attached db
            table = detect_table_in_conn(out_conn, schema_alias=f'{alias}.')
            if not table:
                print(f'  No tables found in {db_path}. Detaching and skipping.')
                out_conn.execute(f"DETACH DATABASE {alias}")
                continue
            print(f'  Detected table: {table}')

            turbine_val = f'{n:02d}'

            if not created:
                # create empty table with same columns + Turbine column
                # Use CREATE TABLE AS SELECT ... WHERE 0 to copy schema
                create_sql = f'CREATE TABLE "Penmanshiel Data" AS SELECT *, "{turbine_val}" AS Turbine FROM {alias}."{table}" WHERE 0'
                out_conn.execute(create_sql)
                out_conn.commit()
                created = True
                print('  Created target table "Penmanshiel Data"')

            # Insert rows from source adding Turbine value
            insert_sql = f'INSERT INTO "Penmanshiel Data" SELECT *, "{turbine_val}" AS Turbine FROM {alias}."{table}"'
            print(f'  Inserting rows for turbine {turbine_val}...')
            out_conn.execute('BEGIN')
            out_conn.execute(insert_sql)
            out_conn.execute('COMMIT')

            # detach
            out_conn.execute(f"DETACH DATABASE {alias}")
            processed.append((n, db_path, table))
            # flush to disk
            out_conn.commit()

        if not created:
            print('No source tables were found; nothing was created.')
            return

        # create index on Turbine
        try:
            out_conn.execute('CREATE INDEX IF NOT EXISTS idx_penmanshiel_turbine ON "Penmanshiel Data" (Turbine)')
            out_conn.commit()
        except Exception as e:
            print('Failed to create index:', e)

        # report counts per turbine
        cur = out_conn.execute('SELECT Turbine, COUNT(*) FROM "Penmanshiel Data" GROUP BY Turbine ORDER BY Turbine')
        rows = cur.fetchall()
        print('\nPer-turbine row counts:')
        total = 0
        for r in rows:
            print(f'  Turbine {r[0]}: {r[1]}')
            total += r[1]
        print(f'Total rows in merged table: {total}')

    finally:
        out_conn.close()


if __name__ == '__main__':
    merge()

