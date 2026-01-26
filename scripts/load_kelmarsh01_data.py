"""
Load turbine data CSVs for Kelmarsh turbine 1 into a SQLite .db with table "Kelmarsh Data".

This mirrors `load_penmanshiel01_data.py` but targets files named
`Turbine_Data_Kelmarsh_1_*.csv` (also accepts '01').

Usage: python scripts/load_kelmarsh01_data.py
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
OUT_DB = OUT_DIR / "kelmarsh_1_data.db"
OUT_TABLE = "Kelmarsh Data"
TURBINE_IDS = ["1"]

# Keep a broad set of target columns (kept short here for readability; reader can handle missing headers)
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


def find_files_for_turbine(turbine_str: str, data_dir: Path) -> List[Path]:
    patterns = [f"Turbine_Data_Kelmarsh_{turbine_str}_*.csv", f"Turbine_Data_Kelmarsh_{int(turbine_str)}_*.csv"]
    files = []
    for p in patterns:
        files.extend(sorted(data_dir.glob(p)))
    # dedupe and ensure correct prefix
    unique = []
    seen = set()
    for f in files:
        name = f.name
        if not (name.startswith(f"Turbine_Data_Kelmarsh_{turbine_str}_") or name.startswith(f"Turbine_Data_Kelmarsh_{int(turbine_str)}_")):
            continue
        if str(f) not in seen:
            unique.append(f)
            seen.add(str(f))
    if unique:
        print(f"Found {len(unique)} Turbine_Data files for Kelmarsh turbine {turbine_str}:")
        for u in unique:
            print("  ", u.name)
    return unique


def read_csv_flexible(path: Path) -> pd.DataFrame:
    read_common = dict(na_values=['-'])
    last_exc = None
    for enc in ("utf-8", "cp1252"):
        for engine in ("c", "python"):
            try:
                sample = pd.read_csv(path, encoding=enc, engine=engine, header=None, comment='#', skipinitialspace=True, low_memory=False, nrows=5, **read_common)
                if sample.shape[1] > 0:
                    first_val = str(sample.iat[0, 0])
                    if re.match(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2})?", first_val):
                        df = pd.read_csv(path, encoding=enc, engine=engine, header=None, comment='#', skipinitialspace=True, low_memory=False, **read_common)
                        ncols = df.shape[1]
                        if ncols <= len(TARGET_COLUMNS):
                            names = TARGET_COLUMNS[:ncols]
                        else:
                            names = TARGET_COLUMNS + [f"col_{i}" for i in range(ncols - len(TARGET_COLUMNS))]
                        df.columns = names
                        return df
                break
            except Exception:
                last_exc = None
    for enc in ("utf-8", "cp1252"):
        for engine in ("c", "python"):
            try:
                df = pd.read_csv(path, encoding=enc, engine=engine, header=8, comment='#', skipinitialspace=True, low_memory=False, **read_common)
                if any(re.match(r"^\d", str(c).strip()) for c in df.columns[:3]):
                    df = pd.read_csv(path, encoding=enc, engine=engine, header=0, comment='#', skipinitialspace=True, low_memory=False, **read_common)
                df.columns = [c.strip() for c in df.columns]
                return df
            except Exception as e:
                last_exc = e
    raise last_exc


def read_all_files(files: List[Path]) -> List[pd.DataFrame]:
    dfs = []
    for f in files:
        print(f"Reading {f.name} ...")
        try:
            df = read_csv_flexible(f)
            dfs.append(df)
        except Exception as e:
            print(f"Failed to read {f.name}: {e}")
    return dfs


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


def concat_and_dedup(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    if not dfs:
        return pd.DataFrame()
    all_df = pd.concat(dfs, ignore_index=True, sort=False)
    before = len(all_df)
    all_df = all_df.drop_duplicates()
    after = len(all_df)
    print(f"Concatenated {len(dfs)} files: {before} rows -> {after} after dropping exact duplicates")
    return all_df


def write_to_sqlite(df: pd.DataFrame, out_db: Path, table_name: str, if_exists: str = 'replace') -> None:
    out_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(out_db))
    try:
        # if_exists passed through so caller can append per-file to avoid high memory
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        try:
            for ts_col in ("Timestamp", "Timestamp start", "time", "Time"):
                if ts_col in df.columns:
                    conn.execute(f'CREATE INDEX IF NOT EXISTS idx_ts ON "{table_name}"("{ts_col}")')
            conn.commit()
        except Exception as e:
            print(f"Warning: failed to create index: {e}")
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


def process_file_in_chunks(path: Path, out_db: Path, table_name: str, first_file: bool, chunksize: int = 100_000) -> int:
    """Read a CSV file in chunks and write each chunk to sqlite to limit memory use.

    Returns number of rows written for this file.
    """
    read_common = dict(na_values=['-'], comment='#', skipinitialspace=True)
    rows_written = 0
    # try encodings and engines
    for enc in ("utf-8", "cp1252"):
        try:
            # sample to detect header style
            sample = pd.read_csv(path, encoding=enc, engine='c', header=None, nrows=5, **read_common)
        except Exception:
            try:
                sample = pd.read_csv(path, encoding=enc, engine='python', header=None, nrows=5, **read_common)
            except Exception:
                continue
        if sample.shape[1] == 0:
            continue
        first_val = str(sample.iat[0, 0])
        # decide header mode and names
        if re.match(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2})?", first_val):
            header_mode = None
            # determine number of columns by reading first row only
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
            # try header=8 then fallback to header=0
            header_mode = 8
            names = None
            try:
                hdrdf = pd.read_csv(path, encoding=enc, engine='c', header=8, nrows=1, **read_common)
            except Exception:
                try:
                    hdrdf = pd.read_csv(path, encoding=enc, engine='python', header=8, nrows=1, **read_common)
                except Exception:
                    # fallback to header=0
                    header_mode = 0
            if header_mode in (0, 8):
                # Let pandas infer column names; we'll strip them later
                names = None
        # Now stream the file in chunks using detected params
        try:
            reader = pd.read_csv(path, encoding=enc, engine='c', header=header_mode, names=names, chunksize=chunksize, low_memory=True, **read_common)
        except Exception:
            try:
                reader = pd.read_csv(path, encoding=enc, engine='python', header=header_mode, names=names, chunksize=chunksize, low_memory=True, **read_common)
            except Exception as e:
                # try next encoding
                continue
        first_chunk = first_file
        for chunk in reader:
            # if header was provided by pandas, ensure column names are stripped
            chunk.columns = [str(c).strip() for c in chunk.columns]
            prepared = select_and_order_columns(chunk, TARGET_COLUMNS)
            if prepared.empty:
                continue
            write_to_sqlite(prepared, out_db, table_name, if_exists='replace' if first_chunk else 'append')
            rows_written += len(prepared)
            first_chunk = False
        # finished streaming for this encoding
        return rows_written
    # if we get here, no encoding worked
    raise RuntimeError(f"Failed to read CSV {path} with available encodings")


def main() -> int:
    files = []
    for t in TURBINE_IDS:
        files.extend(find_files_for_turbine(t, DATA_DIR))
    files = sorted(set(files), key=lambda p: str(p))
    if not files:
        print(f"No turbine data files found for Kelmarsh turbine 1 in {DATA_DIR}")
        return 1

    # Process files sequentially and append to the DB to keep memory usage low
    first = True
    processed_any = False
    total_rows = 0
    for f in files:
        print(f"\nProcessing file: {f.name} ...")
        try:
            written = process_file_in_chunks(f, OUT_DB, OUT_TABLE, first_file=first)
        except Exception as e:
            print(f"Failed to process {f.name}: {e}")
            continue
        print(f"Wrote {written} rows from {f.name} to DB (mode={'replace' if first else 'append'})")
        total_rows += written
        first = False
        processed_any = True

    if not processed_any:
        print("No files were processed successfully. Exiting.")
        return 1

    total, cols, rows = verify_db(OUT_DB, OUT_TABLE, sample=3)
    print(f"Total rows in DB: {total} (expected approximately {total_rows})")
    print(f"Columns in DB ({len(cols)}): {cols[:10]}{'...' if len(cols)>10 else ''}")
    print(f"Example rows (up to 3):")
    for r in rows:
        print(r[:5])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

