"""
Load Penmanshiel status CSVs for turbines 1..15 into one SQLite .db with table "Penmanshiel Status".
- Scans data/penmanshiel_data for Status_Penmanshiel_{n}_*.csv for n in 1..15
- Reads CSVs with encoding fallbacks and skips commented header lines
- Adds integer column 'Turbine' (from filename / turbine id)
- Concatenates, drops exact duplicates, removes 'Custom contract category' column if present
- Writes a single DB file at data/sqlitedbs/penmanshiel_all_status.db with table "Penmanshiel Status"

Usage: python scripts/load_penmanshiel_status.py
"""
from pathlib import Path
import sqlite3
from typing import List, Tuple
import re

try:
    import pandas as pd
except Exception:
    print("This script requires pandas. Install with: pip install pandas")
    raise

ROOT = Path(__file__).resolve().parents[1]
PENM_DIR = ROOT / "data" / "penmanshiel_data"
OUT_DIR = ROOT / "data" / "sqlitedbs"
OUT_DB = OUT_DIR / "penmanshiel_all_status.db"
OUT_TABLE = "Penmanshiel Status"
TURBINE_IDS = range(1, 16)  # 1..15 inclusive
CSV_NAME_RE = re.compile(r"Status_Penmanshiel_(\d+)_")


def find_files_for_turbine(turbine: int, data_dir: Path) -> List[Path]:
    # Match both '1' and '01' style filenames to be robust.
    pattern1 = f"Status_Penmanshiel_{turbine}_*.csv"
    pattern2 = f"Status_Penmanshiel_{turbine:02d}_*.csv"
    files = sorted(data_dir.glob(pattern1))
    if not files:
        files = sorted(data_dir.glob(pattern2))
    # also allow any files that contain the turbine id as two-digit in other positions
    return files


def read_csv_flexible(path: Path) -> pd.DataFrame:
    """Read CSV with fallbacks for encoding and skip commented header lines. Parse timestamp columns if present."""
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


def read_all_turbines(turbine_ids: range, data_dir: Path) -> List[pd.DataFrame]:
    frames: List[pd.DataFrame] = []
    for tid in turbine_ids:
        files = find_files_for_turbine(tid, data_dir)
        if not files:
            print(f"No files found for Turbine {tid} in {data_dir}")
            continue
        for f in files:
            print(f"Reading Turbine {tid} file: {f.name} ...")
            try:
                df = read_csv_flexible(f)
                df['Turbine'] = tid
                frames.append(df)
            except Exception as e:
                print(f"  Failed to read {f.name}: {e}")
    return frames


def concat_and_clean(frames: List[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    all_df = pd.concat(frames, ignore_index=True, sort=False)
    before = len(all_df)
    all_df = all_df.drop_duplicates()
    after = len(all_df)
    if 'Custom contract category' in all_df.columns:
        all_df = all_df.drop(columns=['Custom contract category'])
        print("Dropped column 'Custom contract category'")
    print(f"Concatenated {len(frames)} files: {before} rows -> {after} after dropping exact duplicates")
    return all_df


def write_to_sqlite(df: pd.DataFrame, out_db: Path, table_name: str) -> None:
    out_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(out_db))
    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        # create helpful indexes if columns exist
        try:
            if 'Timestamp start' in df.columns:
                conn.execute(f'CREATE INDEX IF NOT EXISTS idx_ts_start ON "{table_name}"("Timestamp start")')
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_turbine ON "{table_name}"("Turbine")')
            conn.commit()
        except Exception as e:
            print(f"Warning: failed to create indexes: {e}")
    finally:
        conn.close()


def verify_db(out_db: Path, table_name: str, sample: int = 5) -> Tuple[int, List[str], List[Tuple]]:
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
    if not PENM_DIR.exists():
        print(f"Penmanshiel data directory not found: {PENM_DIR}")
        return 1

    frames = read_all_turbines(TURBINE_IDS, PENM_DIR)
    if not frames:
        print("No data read for any turbines. Exiting.")
        return 1

    all_df = concat_and_clean(frames)
    if all_df.empty:
        print("No data to write after concatenation/dedup. Exiting.")
        return 1

    write_to_sqlite(all_df, OUT_DB, OUT_TABLE)

    total, cols, rows = verify_db(OUT_DB, OUT_TABLE, sample=5)
    print(f"Wrote {total} rows to {OUT_DB} table '{OUT_TABLE}'")
    print("Columns:", cols)
    print("Sample rows:")
    for r in rows:
        print(r)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
