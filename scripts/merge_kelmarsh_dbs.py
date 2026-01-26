"""
Merge per-turbine Kelmarsh sqlite DBs into a single DB with an added 'Turbine' column.

- Finds files in data/sqlitedbs named kelmarsh_<n>_data.db (n numeric)
- Reads the table 'Kelmarsh Data' from each DB in chunks to avoid excessive memory use
- Adds an integer column 'Turbine' with the turbine number
- Writes/appends data into data/sqlitedbs/kelmarsh_all_data.db table 'Kelmarsh Data'

Usage: python scripts/merge_kelmarsh_dbs.py
"""
from pathlib import Path
import re
import sqlite3
from typing import List

try:
    import pandas as pd
except Exception:
    print("This script requires pandas. Install with: pip install pandas")
    raise

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "data" / "sqlitedbs"
OUT_DB = SQL_DIR / "kelmarsh_all_data.db"
SRC_PATTERN = re.compile(r"^kelmarsh_(\d+)_data\.db$")
SRC_TABLE = "Kelmarsh Data"
CHUNKSIZE = 100_000


def find_source_dbs(sql_dir: Path) -> List[Path]:
    files = []
    for p in sorted(sql_dir.iterdir()):
        m = SRC_PATTERN.match(p.name)
        if m:
            files.append(p)
    return files


def get_table_columns(db_path: Path, table: str) -> List[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if not cur.fetchone():
            return []
        cur.execute(f'PRAGMA table_info("{table}")')
        cols = [r[1] for r in cur.fetchall()]
        return cols
    finally:
        conn.close()


def merge_databases():
    srcs = find_source_dbs(SQL_DIR)
    if not srcs:
        print("No source kelmarsh_*_data.db files found in", SQL_DIR)
        return 1
    print(f"Found {len(srcs)} source DBs:")
    for s in srcs:
        print("  ", s.name)

    # Determine canonical column order from the first DB that contains the table
    canonical_cols = None
    ordered_extra = []

    for s in srcs:
        cols = get_table_columns(s, SRC_TABLE)
        if not cols:
            print(f"Warning: table '{SRC_TABLE}' not found in {s.name}; skipping")
            continue
        if canonical_cols is None:
            canonical_cols = list(cols)
        else:
            # detect any new cols not in canonical and append them
            for c in cols:
                if c not in canonical_cols and c not in ordered_extra:
                    ordered_extra.append(c)
    if canonical_cols is None:
        print(f"No source DBs contain table '{SRC_TABLE}'. Nothing to merge.")
        return 1

    final_cols = canonical_cols + ordered_extra
    # add Turbine as the last column
    if 'Turbine' in final_cols:
        print("Note: 'Turbine' column already present in canonical columns; it will be overwritten with source turbine id")
    else:
        final_cols = final_cols + ['Turbine']

    # Remove existing output DB if present (we'll rewrite)
    if OUT_DB.exists():
        print(f"Removing existing output DB {OUT_DB.name}")
        OUT_DB.unlink()

    total_rows = 0
    first_write = True

    for s in srcs:
        m = SRC_PATTERN.match(s.name)
        if not m:
            continue
        tid = int(m.group(1))
        cols = get_table_columns(s, SRC_TABLE)
        if not cols:
            print(f"Skipping {s.name} (no table {SRC_TABLE})")
            continue
        print(f"Merging turbine {tid} from {s.name} ...")
        conn = sqlite3.connect(str(s))
        try:
            # stream rows from the source table
            for chunk in pd.read_sql_query(f'SELECT * FROM "{SRC_TABLE}"', conn, chunksize=CHUNKSIZE):
                # ensure columns exist in chunk
                chunk.columns = [str(c).strip() for c in chunk.columns]
                # reindex to canonical (without Turbine), extra columns will be included if present
                base_cols = final_cols[:-1]
                # add any columns from chunk that are not in base_cols to the end (preserve data)
                missing_cols = [c for c in chunk.columns if c not in base_cols]
                ordered = base_cols + [c for c in missing_cols if c not in base_cols]
                # reindex the chunk to the ordered set (add missing columns filled with NA)
                chunk = chunk.reindex(columns=ordered)
                # ensure chunk columns align with final_cols minus Turbine; if some canonical cols missing they will be present as NaN
                # add Turbine column
                chunk['Turbine'] = tid
                # write to output DB
                # first write: replace, subsequent: append
                mode = 'replace' if first_write else 'append'
                out_conn = sqlite3.connect(str(OUT_DB))
                try:
                    chunk.to_sql(SRC_TABLE, out_conn, if_exists=mode, index=False)
                finally:
                    out_conn.close()
                rows = len(chunk)
                total_rows += rows
                first_write = False
        finally:
            conn.close()
    print(f"Merging complete. Total rows written: {total_rows}")
    # final verification
    conn = sqlite3.connect(str(OUT_DB))
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{SRC_TABLE}"')
        total = cur.fetchone()[0]
        cur.execute(f'PRAGMA table_info("{SRC_TABLE}")')
        cols = [r[1] for r in cur.fetchall()]
        print(f"Output DB {OUT_DB.name} has {total} rows and columns: {cols[:10]}{'...' if len(cols)>10 else ''}")
    finally:
        conn.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(merge_databases())

