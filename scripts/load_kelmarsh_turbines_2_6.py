"""
Create per-turbine SQLite DBs for Kelmarsh turbines 2..6.

This reuses the robust CSV streaming approach: detect header style, stream in chunks,
select target columns, and append chunks to a per-turbine DB to keep memory low.

Usage: python scripts/load_kelmarsh_turbines_2_6.py
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
DATA_DIR = ROOT / "data" / "kelmarsh_data"
OUT_DIR = ROOT / "data" / "sqlitedbs"
OUT_TABLE = "Kelmarsh Data"
TURBINE_RANGE = range(2, 7)  # 2..6

# Keep the same small TARGET_COLUMNS used for turbine 1 for consistency
TARGET_COLUMNS = [
    "Date and time",
    "Wind speed (m/s)",
    "Wind speed, Standard deviation (m/s)",
    "Wind speed, Minimum (m/s)",
    "Wind speed, Maximum (m/s)",
    "Long Term Wind (m/s)",
    "Wind speed Sensor 1 (m/s)",
    "Wind speed Sensor 1, Standard deviation (m/s)",
    "Power (kW)",
]


def find_files_for_turbine(turbine_id: int, data_dir: Path) -> List[Path]:
    pattern = f"Turbine_Data_Kelmarsh_{turbine_id}_*.csv"
    files = sorted(data_dir.glob(pattern))
    if files:
        print(f"Found {len(files)} files for turbine {turbine_id}")
        for f in files:
            print("  ", f.name)
    return files


def select_and_order_columns(df: pd.DataFrame, target_cols: List[str]) -> pd.DataFrame:
    cols = list(df.columns)
    normalized_map = {}
    for c in cols:
        norm = re.sub(r"\W+", "", c).lower()
        normalized_map[norm] = c
    selected_series = []
    for tc in target_cols:
        found = None
        for c in cols:
            if c.strip().lower() == tc.strip().lower():
                found = c
                break
        if not found:
            norm_tc = re.sub(r"\W+", "", tc).lower()
            if norm_tc in normalized_map:
                found = normalized_map[norm_tc]
        if not found:
            words = [w for w in re.split(r"\W+", tc.lower()) if w]
            for c in cols:
                cl = c.lower()
                if all(w in cl for w in words):
                    found = c
                    break
        if found:
            selected_series.append(df[found].rename(tc))
        else:
            selected_series.append(pd.Series([pd.NA] * len(df), name=tc))
    out_df = pd.concat(selected_series, axis=1)
    out_df = out_df[target_cols]
    return out_df


def write_to_sqlite(df: pd.DataFrame, out_db: Path, table_name: str, if_exists: str = 'append') -> None:
    out_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(out_db))
    try:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    finally:
        conn.close()


def process_file_in_chunks(path: Path, out_db: Path, table_name: str, first_file: bool, chunksize: int = 100_000) -> int:
    read_common = dict(na_values=['-'], comment='#', skipinitialspace=True)
    rows_written = 0
    for enc in ("utf-8", "cp1252"):
        try:
            sample = pd.read_csv(path, encoding=enc, engine='c', header=None, nrows=5, **read_common)
        except Exception:
            try:
                sample = pd.read_csv(path, encoding=enc, engine='python', header=None, nrows=5, **read_common)
            except Exception:
                continue
        if sample.shape[1] == 0:
            continue
        first_val = str(sample.iat[0, 0])
        if re.match(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2})?", first_val):
            header_mode = None
            try:
                row0 = pd.read_csv(path, encoding=enc, engine='c', header=None, nrows=1, **read_common)
            except Exception:
                row0 = pd.read_csv(path, encoding=enc, engine='python', header=None, nrows=1, **read_common)
            ncols = row0.shape[1]
            if ncols <= len(TARGET_COLUMNS):
                names = TARGET_COLUMNS[:ncols]
            else:
                names = TARGET_COLUMNS + [f"col_{i}" for i in range(ncols - len(TARGET_COLUMNS))]
        else:
            header_mode = 8
            names = None
            try:
                pd.read_csv(path, encoding=enc, engine='c', header=8, nrows=1, **read_common)
            except Exception:
                try:
                    pd.read_csv(path, encoding=enc, engine='python', header=8, nrows=1, **read_common)
                except Exception:
                    header_mode = 0
        try:
            reader = pd.read_csv(path, encoding=enc, engine='c', header=header_mode, names=names, chunksize=chunksize, low_memory=True, **read_common)
        except Exception:
            try:
                reader = pd.read_csv(path, encoding=enc, engine='python', header=header_mode, names=names, chunksize=chunksize, low_memory=True, **read_common)
            except Exception:
                continue
        first_chunk = first_file
        for chunk in reader:
            chunk.columns = [str(c).strip() for c in chunk.columns]
            prepared = select_and_order_columns(chunk, TARGET_COLUMNS)
            if prepared.empty:
                continue
            write_to_sqlite(prepared, out_db, table_name, if_exists='replace' if first_chunk else 'append')
            rows_written += len(prepared)
            first_chunk = False
        return rows_written
    raise RuntimeError(f"Failed to read CSV {path} with available encodings")


def verify_db(out_db: Path, table_name: str, sample: int = 3) -> Tuple[int, List[str], List[Tuple]]:
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
    any_processed = False
    for tid in TURBINE_RANGE:
        print(f"\n=== Processing Kelmarsh turbine {tid} ===")
        files = find_files_for_turbine(tid, DATA_DIR)
        if not files:
            print(f"No files found for turbine {tid}, skipping.")
            continue
        out_db = OUT_DIR / f"kelmarsh_{tid}_data.db"
        first = True
        total_written = 0
        for f in files:
            print(f"Processing file: {f.name} ...")
            try:
                written = process_file_in_chunks(f, out_db, OUT_TABLE, first_file=first)
            except Exception as e:
                print(f"Failed to process {f.name}: {e}")
                continue
            print(f"Wrote {written} rows (mode={'replace' if first else 'append'})")
            total_written += written
            first = False
        if total_written:
            total, cols, rows = verify_db(out_db, OUT_TABLE, sample=3)
            print(f"Finished turbine {tid}: total rows in DB {total} (approx {total_written})")
            any_processed = True
    return 0 if any_processed else 1


if __name__ == '__main__':
    raise SystemExit(main())

