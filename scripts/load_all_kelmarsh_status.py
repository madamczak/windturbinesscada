"""
Load all Kelmarsh turbines' status CSVs into individual SQLite .db files.
- Scans data/kelmarsh_data for Status_Kelmarsh_{n}_*.csv
- Concatenates per turbine, drops the 'Custom contract category' column if present
- Writes to data/sqlitedbs/kelmarsh_{n}_status.db with table name "Kelmarsh {n} Status"

Usage: python scripts/load_all_kelmarsh_status.py
"""
from pathlib import Path
import re
import sqlite3
from typing import List, Dict, Tuple, Optional

try:
    import pandas as pd
except Exception:
    print("This script requires pandas. Install with: pip install pandas")
    raise

ROOT = Path(__file__).resolve().parents[1]
KELMARSH_DIR = ROOT / "data" / "kelmarsh_data"
OUT_DIR = ROOT / "data" / "sqlitedbs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATTERN = re.compile(r"Status_Kelmarsh_(\d+)_")


def find_all_status_files(kelmarsh_dir: Path) -> Dict[str, List[Path]]:
    """Return a mapping turbine_id -> list of matching CSV paths."""
    groups: Dict[str, List[Path]] = {}
    for p in sorted(kelmarsh_dir.glob("Status_Kelmarsh_*.csv")):
        m = CSV_PATTERN.search(p.name)
        if not m:
            continue
        tid = m.group(1)
        groups.setdefault(tid, []).append(p)
    return groups


def read_csv_flexible(path: Path) -> pd.DataFrame:
    """Read CSV with encoding fallbacks and skip commented header lines."""
    read_opts = dict(comment='#', na_values=['-'], engine='python')
    parse_dates = ["Timestamp start", "Timestamp end"]
    last_exc = None
    for enc in ("utf-8", "cp1252"):
        try:
            df = pd.read_csv(path, encoding=enc, parse_dates=parse_dates, **read_opts)
            df.columns = [c.strip() for c in df.columns]
            return df
        except Exception as e:
            last_exc = e
    raise last_exc


def concat_and_clean(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Concat list of DataFrames, drop exact duplicates, and remove the 'Custom contract category' column if present."""
    if not dfs:
        return pd.DataFrame()
    all_df = pd.concat(dfs, ignore_index=True, sort=False)
    before = len(all_df)
    all_df = all_df.drop_duplicates()
    after = len(all_df)
    # Drop the last column if it's named 'Custom contract category'
    if 'Custom contract category' in all_df.columns:
        all_df = all_df.drop(columns=['Custom contract category'])
        print("Dropped column 'Custom contract category'")
    print(f"Concatenated {len(dfs)} files: {before} rows -> {after} after dropping exact duplicates")
    return all_df


def write_db(df: pd.DataFrame, out_db: Path, table_name: str) -> None:
    out_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(out_db))
    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        if 'Timestamp start' in df.columns:
            try:
                conn.execute(f'CREATE INDEX IF NOT EXISTS idx_ts_start ON "{table_name}"("Timestamp start")')
                conn.commit()
            except Exception as e:
                print(f"Warning: failed to create index: {e}")
    finally:
        conn.close()


def verify_db(out_db: Path, table_name: str) -> Tuple[int, List[str]]:
    conn = sqlite3.connect(str(out_db))
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        total = cur.fetchone()[0]
        cur.execute(f'PRAGMA table_info("{table_name}")')
        cols = [r[1] for r in cur.fetchall()]
        return total, cols
    finally:
        conn.close()


def process_all():
    groups = find_all_status_files(KELMARSH_DIR)
    if not groups:
        print(f"No Status_Kelmarsh files found in {KELMARSH_DIR}")
        return 1

    for tid, files in groups.items():
        print(f"\nProcessing Kelmarsh turbine {tid}: {len(files)} files")
        dfs = []
        for f in files:
            print(f"  Reading {f.name} ...")
            try:
                df = read_csv_flexible(f)
                dfs.append(df)
            except Exception as e:
                print(f"  Failed to read {f.name}: {e}")
        if not dfs:
            print(f"  No data read for turbine {tid}, skipping")
            continue
        all_df = concat_and_clean(dfs)
        if all_df.empty:
            print(f"  No rows after concat/dedup for turbine {tid}, skipping")
            continue
        out_db = OUT_DIR / f"kelmarsh_{tid}_status.db"
        table_name = f"Kelmarsh {tid} Status"
        write_db(all_df, out_db, table_name)
        total, cols = verify_db(out_db, table_name)
        print(f"  Wrote {total} rows to {out_db} table '{table_name}'")
        print(f"  Columns: {cols}")

    return 0


if __name__ == '__main__':
    raise SystemExit(process_all())

