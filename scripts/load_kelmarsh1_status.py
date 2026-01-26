"""
Concatenate Status_Kelmarsh_1_*.csv into a single SQLite table named "Kelmarsh 1 Status".
Creates database at: data/kelmarsh_data/kelmarsh1_status.sqlite

Usage: python scripts/load_kelmarsh1_status.py
"""
from pathlib import Path
import sqlite3
import sys
from typing import List, Optional, Tuple

try:
    import pandas as pd
except Exception:
    print("This script requires pandas. Install with: pip install pandas")
    raise

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "kelmarsh_data"
OUT_DB = DATA_DIR / "kelmarsh1_status.sqlite"
TABLE_NAME = "Kelmarsh 1 Status"


def find_status_files(data_dir: Path, pattern: str = "Status_Kelmarsh_1_*.csv") -> List[Path]:
    """Return a sorted list of matching CSV files."""
    files = sorted(data_dir.glob(pattern))
    return files


def read_csv_flexible(path: Path) -> pd.DataFrame:
    """Read one CSV with fallbacks for encoding and skip commented header lines.

    Returns a pandas.DataFrame. Raises the last exception if all attempts fail.
    """
    # Try utf-8 then cp1252; skip commented header lines starting with '#'
    read_opts = dict(comment='#', na_values=['-'], engine='python')
    parse_dates = ["Timestamp start", "Timestamp end"]
    last_exc = None
    for enc in ("utf-8", "cp1252"):
        try:
            df = pd.read_csv(path, encoding=enc, parse_dates=parse_dates, **read_opts)
            # strip column names
            df.columns = [c.strip() for c in df.columns]
            return df
        except Exception as e:
            last_exc = e
    raise last_exc


def read_all_files(files: List[Path]) -> List[pd.DataFrame]:
    """Read all files and return list of DataFrames; errors for individual files are printed but do not abort."""
    dfs: List[pd.DataFrame] = []
    for f in files:
        print(f"Reading {f.name} ...")
        try:
            df = read_csv_flexible(f)
            dfs.append(df)
        except Exception as e:
            print(f"Failed to read {f}: {e}")
    return dfs


def concat_and_dedup(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate list of DataFrames and drop exact duplicates."""
    if not dfs:
        return pd.DataFrame()
    all_df = pd.concat(dfs, ignore_index=True, sort=False)
    before = len(all_df)
    all_df = all_df.drop_duplicates()
    after = len(all_df)
    print(f"Concatenated {len(dfs)} files: {before} rows -> {after} after dropping exact duplicates")
    return all_df


def write_to_sqlite(df: pd.DataFrame, out_db: Path, table_name: str) -> None:
    """Write DataFrame to SQLite database (replace existing table) and create an index on Timestamp start if present."""
    conn = sqlite3.connect(str(out_db))
    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        if 'Timestamp start' in df.columns:
            try:
                conn.execute(f'CREATE INDEX IF NOT EXISTS idx_kelmarsh1_ts_start ON "{table_name}"("Timestamp start")')
                conn.commit()
            except Exception as e:
                print(f"Warning: failed to create index: {e}")
    finally:
        conn.close()


def verify_db(out_db: Path, table_name: str, sample: int = 5) -> Tuple[int, List[str], List[Tuple[Optional[str], ...]]]:
    """Open the DB and return (count, columns, sample_rows)."""
    conn = sqlite3.connect(str(out_db))
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        total = cur.fetchone()[0]
        cur.execute(f'PRAGMA table_info("{table_name}")')
        cols = [r[1] for r in cur.fetchall()]
        cur.execute(f'SELECT * FROM "{table_name}" LIMIT {sample}')
        rows = cur.fetchall()
        return total, cols, rows
    finally:
        conn.close()


def main() -> int:
    """Orchestrate the steps: find, read, concat, write, verify. Returns exit code."""
    files = find_status_files(DATA_DIR)
    if not files:
        print(f"No files found in {DATA_DIR} matching Status_Kelmarsh_1_*.csv")
        return 1

    dfs = read_all_files(files)
    if not dfs:
        print("No dataframes read successfully. Exiting.")
        return 1

    all_df = concat_and_dedup(dfs)
    if all_df.empty:
        print("No data to write after concatenation/dedup. Exiting.")
        return 1

    write_to_sqlite(all_df, OUT_DB, TABLE_NAME)

    total, cols, rows = verify_db(OUT_DB, TABLE_NAME, sample=5)
    print(f"Wrote {total} rows to {OUT_DB} table '{TABLE_NAME}'")
    print("Sample columns:", cols)
    for r in rows:
        print(r)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
