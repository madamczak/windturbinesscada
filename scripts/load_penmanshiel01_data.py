"""
Load turbine data CSVs for Penmanshiel turbine 01 into a SQLite .db with table "Penmanshiel Data".

Steps (one function per step):
- find_files_for_turbine: discover CSV files for turbine '01' (also accepts '1')
- read_csv_flexible: read a CSV robustly (encoding fallbacks, skip comment lines)
- read_all_files: read every file into DataFrames
- concat_and_clean: concatenate and drop exact duplicates
- write_to_sqlite: write DataFrame to sqlite .db
- verify_db: open DB and return counts and sample rows

Usage: python scripts/load_penmanshiel01_data.py
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
DATA_DIR = ROOT / "data" / "penmanshiel_data"
OUT_DIR = ROOT / "data" / "sqlitedbs"
OUT_DB = OUT_DIR / "penmanshiel_01_data.db"
OUT_TABLE = "Penmanshiel Data"
TURBINE_IDS = ["01", "1"]

# target columns moved to module-level so reader can assign names when files don't include a header
TARGET_COLUMNS = [
    "Date and time",
    "Wind speed (m/s)",
    "Wind speed, Standard deviation (m/s)",
    "Wind speed, Minimum (m/s)",
    "Wind speed, Maximum (m/s)",
    "Long Term Wind (m/s)",
    "Wind speed Sensor 1 (m/s)",
    "Wind speed Sensor 1, Standard deviation (m/s)",
    "Wind speed Sensor 1, Minimum (m/s)",
    "Wind speed Sensor 1, Maximum (m/s)",
    "Wind speed Sensor 2 (m/s)",
    "Wind speed Sensor 2, Standard deviation (m/s)",
    "Wind speed Sensor 2, Minimum (m/s)",
    "Wind speed Sensor 2, Maximum (m/s)",
    "Density adjusted wind speed (m/s)",
    "Wind direction (°)",
    "Nacelle position (°)",
    "Wind direction, Standard deviation (°)",
    "Wind direction, Minimum (°)",
    "Wind direction, Maximum (°)",
    "Nacelle position, Standard deviation (°)",
    "Nacelle position, Minimum (°)",
    "Nacelle position, Maximum (°)",
    "Vane position 1+2 (°)",
    "Vane position 1+2, Max (°)",
    "Vane position 1+2, Min (°)",
    "Vane position 1+2, StdDev (°)",
    "Energy Export (kWh)",
    "Energy Export counter (kWh)",
    "Energy Import (kWh)",
    "Energy Import counter (kWh)",
    "Lost Production (Contractual) (kWh)",
    "Lost Production (Time-based IEC B.2.2) (kWh)",
    "Lost Production (Time-based IEC B.2.3) (kWh)",
    "Lost Production (Time-based IEC B.2.4) (kWh)",
    "Lost Production (Time-based IEC B.3.2) (kWh)",
    "Lost Production (Production-based IEC B.2.2) (kWh)",
    "Lost Production (Production-based IEC B.2.3) (kWh)",
    "Lost Production (Production-based IEC B.3.2) (kWh)",
    "Energy Budget - Default (kWh)",
    "Energy Theoretical (kWh)",
    "Lost Production to Downtime (kWh)",
    "Lost Production to Performance (kWh)",
    "Lost Production Total (kWh)",
    "Lost Production to Curtailment (Total) (kWh)",
    "Lost Production to Curtailment (Grid) (kWh)",
    "Lost Production to Curtailment (Noise) (kWh)",
    "Lost Production to Curtailment (Shadow) (kWh)",
    "Lost Production to Curtailment (Bats) (kWh)",
    "Lost Production to Curtailment (Birds) (kWh)",
    "Lost Production to Curtailment (Ice) (kWh)",
    "Lost Production to Curtailment (Sector Management) (kWh)",
    "Lost Production to Curtailment (Technical) (kWh)",
    "Lost Production to Curtailment (Marketing) (kWh)",
    "Lost Production to Curtailment (Boat Action) (kWh)",
    "Compensated Lost Production (kWh)",
    "Virtual Production (kWh)",
    "Lost Production to Curtailment (Grid Constraint) (kWh)",
    "Lost Production to Downtime and Curtailment Total (kWh)",
    "Lost Production (Contractual Global) (kWh)",
    "Lost Production (Contractual Custom) (kWh)",
    "Power (kW)",
    "Potential power default PC (kW)",
    "Power, Standard deviation (kW)",
    "Power, Minimum (kW)",
    "Power, Maximum (kW)",
    "Potential power learned PC (kW)",
    "Potential power reference turbines (kW)",
    "Cascading potential power (kW)",
    "Cascading potential power for performance (kW)",
    "Potential power met mast anemometer (kW)",
    "Potential power primary reference turbines (kW)",
    "Potential power secondary reference turbines (kW)",
    "Turbine Power setpoint (kW)",
    "Potential power estimated (kW)",
    "Potential power MPC (kW)",
    "Potential power met mast anemometer MPC (kW)",
    "Turbine Power setpoint, Max (kW)",
    "Turbine Power setpoint, Min (kW)",
    "Turbine Power setpoint, StdDev (kW)",
    "Available Capacity for Production (kW)",
    "Available Capacity for Production (Planned) (kW)",
    "APE-2 (kW)",
    "Power factor (cosphi)",
    "Power factor (cosphi), Max",
    "Power factor (cosphi), Min",
    "Power factor (cosphi), Standard deviation",
    "Reactive power (kvar)",
    "Reactive power, Max (kvar)",
    "Reactive power, Min (kvar)",
    "Reactive power, Standard deviation (kvar)",
    "Front bearing temperature (°C)",
    "Rear bearing temperature (°C)",
    "Stator temperature 1 (°C)",
    "Nacelle ambient temperature (°C)",
    "Nacelle temperature (°C)",
    "Transformer temperature (°C)",
    "Gear oil inlet temperature (°C)",
    "Generator bearing rear temperature (°C)",
    "Generator bearing front temperature (°C)",
    "Gear oil temperature (°C)",
    "Temp. top box (°C)",
    "Hub temperature (°C)",
    "Ambient temperature (converter) (°C)",
    "Rotor bearing temp (°C)",
    "Transformer cell temperature (°C)",
    "Front bearing temperature, Max (°C)",
    "Front bearing temperature, Min (°C)",
    "Front bearing temperature, Standard deviation (°C)",
    "Rear bearing temperature, Max (°C)",
    "Rear bearing temperature, Min (°C)",
    "Rear bearing temperature, Standard deviation (°C)",
    "Temperature motor axis 1 (°C)",
    "Temperature motor axis 2 (°C)",
    "Temperature motor axis 3 (°C)",
    "CPU temperature (°C)",
    "Nacelle temperature, Max (°C)",
    "Nacelle temperature, Min (°C)",
    "Generator bearing front temperature, Max (°C)",
    "Generator bearing front temperature, Min (°C)",
    "Generator bearing rear temperature, Max (°C)",
    "Generator bearing rear temperature, Min (°C)",
    "Generator bearing front temperature, Std (°C)",
    "Generator bearing rear temperature, Std (°C)",
    "Nacelle temperature, Standard deviation (°C)",
    "Gear oil temperature, Max (°C)",
    "Gear oil temperature, Min (°C)",
    "Gear oil temperature, Standard deviation (°C)",
    "Hub temperature, min (°C)",
    "Hub temperature, max (°C)",
    "Hub temperature, standard deviation (°C)",
    "Ambient temperature (converter), Max (°C)",
    "Ambient temperature (converter), Min (°C)",
    "Ambient temperature (converter), StdDev (°C)",
    "Gear oil inlet temperature, Max (°C)",
    "Gear oil inlet temperature, Min (°C)",
    "Gear oil inlet temperature, StdDev (°C)",
    "Nacelle ambient temperature, Max (°C)",
    "Nacelle ambient temperature, Min (°C)",
    "Nacelle ambient temperature, StdDev (°C)",
    "Rotor bearing temp, Max (°C)",
    "Rotor bearing temp, Min (°C)",
    "Rotor bearing temp, StdDev (°C)",
    "CPU temperature, Max (°C)",
    "CPU temperature, Min (°C)",
    "CPU temperature, StdDev (°C)",
    "Transformer cell temperature, Max (°C)",
    "Transformer cell temperature, Min (°C)",
    "Transformer cell temperature, StdDev (°C)",
    "Transformer temperature, Max (°C)",
    "Transformer temperature, Min (°C)",
    "Transformer temperature, StdDev (°C)",
    "Stator temperature 1, Max (°C)",
    "Stator temperature 1, Min (°C)",
    "Stator temperature 1, StdDev (°C)",
    "Temp. top box, Max (°C)",
    "Temp. top box, Min (°C)",
    "Temp. top box, StdDev (°C)",
    "Temperature motor axis 1, Max (°C)",
    "Temperature motor axis 1, Min (°C)",
    "Temperature motor axis 1, StdDev (°C)",
    "Temperature motor axis 2, Max (°C)",
    "Temperature motor axis 2, Min (°C)",
    "Temperature motor axis 2, StdDev (°C)",
    "Temperature motor axis 3, Max (°C)",
    "Temperature motor axis 3, Min (°C)",
    "Temperature motor axis 3, StdDev (°C)",
    "Voltage L1 / U (V)",
    "Voltage L2 / V (V)",
    "Voltage L3 / W (V)",
    "Grid voltage (V)",
    "Grid voltage, Max (V)",
    "Grid voltage, Min (V)",
    "Grid voltage, Standard deviation (V)",
    "Voltage L1 / U, Min (V)",
    "Voltage L1 / U, Max (V)",
    "Voltage L1 / U, Standard deviation (V)",
    "Voltage L2 / V, Min (V)",
    "Voltage L2 / V, Max (V)",
    "Voltage L2 / V, Standard deviation (V)",
    "Voltage L3 / W, Min (V)",
    "Voltage L3 / W, Max (V)",
    "Voltage L3 / W, Standard deviation (V)",
    "Current L1 / U (A)",
    "Current L2 / V (A)",
    "Current L3 / W (A)",
    "Grid current (A)",
    "Motor current axis 1 (A)",
    "Motor current axis 2 (A)",
    "Motor current axis 3 (A)",
    "Current L1 / U, min (A)",
    "Current L1 / U, max (A)",
    "Current L1 / U, StdDev (A)",
    "Current L2 / V, max (A)",
    "Current L3 / W, max (A)",
    "Current L2 / V, min (A)",
    "Current L2 / V, StdDev (A)",
    "Current L3 / W, min (A)",
    "Current L3 / W, StdDev (A)",
    "Grid current, Max (A)",
    "Grid current, Min (A)",
    "Grid current, StdDev (A)",
    "Motor current axis 1, Max (A)",
    "Motor current axis 1, Min (A)",
    "Motor current axis 1, StdDev (A)",
    "Motor current axis 2, Max (A)",
    "Motor current axis 2, Min (A)",
    "Motor current axis 2, StdDev (A)",
    "Motor current axis 3, Max (A)",
    "Motor current axis 3, Min (A)",
    "Motor current axis 3, StdDev (A)",
    "Rotor speed (RPM)",
    "Generator RPM (RPM)",
    "Gearbox speed (RPM)",
    "Generator RPM, Max (RPM)",
    "Generator RPM, Min (RPM)",
    "Generator RPM, Standard deviation (RPM)",
    "Rotor speed, Max (RPM)",
    "Rotor speed, Min (RPM)",
    "Rotor speed, Standard deviation (RPM)",
    "Gearbox speed, Max (RPM)",
    "Gearbox speed, Min (RPM)",
    "Gearbox speed, StdDev (RPM)",
    "Capacity factor",
    "Data Availability",
    "Time-based Contractual Avail.",
    "Time-based IEC B.2.2 (Users View)",
    "Time-based IEC B.2.3 (Users View)",
    "Time-based IEC B.2.4 (Users View)",
    "Time-based IEC B.3.2 (Manufacturers View)",
    "Production-based IEC B.2.2 (Users View)",
    "Production-based IEC B.2.3 (Users View)",
    "Production-based IEC B.3.2 (Manufacturers View)",
    "Time-based System Avail.",
    "Production-based System Avail.",
    "Production-based Contractual Avail.",
    "Time-based System Avail. (Planned)",
    "Production-based System Avail. (virtual)",
    "Time-based Contractual Avail. (Global)",
    "Time-based Contractual Avail. (Custom)",
    "Production-based Contractual Avail. (Global)",
    "Production-based Contractual Avail. (Custom)",
    "Reactive Energy Export (kvarh)",
    "Reactive Energy Export counter (kvarh)",
    "Reactive Energy Import (kvarh)",
    "Reactive Energy Import counter (kvarh)",
    "Blade angle (pitch position) A (°)",
    "Blade angle (pitch position) B (°)",
    "Blade angle (pitch position) C (°)",
    "Yaw bearing angle (°)",
    "Blade angle (pitch position) A, Max (°)",
    "Blade angle (pitch position) A, Min (°)",
    "Blade angle (pitch position) A, Standard deviation (°)",
    "Blade angle (pitch position) B, Max (°)",
    "Blade angle (pitch position) B, Min (°)",
    "Blade angle (pitch position) B, Standard deviation (°)",
    "Blade angle (pitch position) C, Max (°)",
    "Blade angle (pitch position) C, Min (°)",
    "Blade angle (pitch position) C, Standard deviation (°)",
    "Yaw bearing angle, Max (°)",
    "Yaw bearing angle, Min (°)",
    "Yaw bearing angle, StdDev (°)",
    "Gear oil inlet pressure (bar)",
    "Gear oil pump pressure (bar)",
    "Gear oil inlet pressure, Max (bar)",
    "Gear oil inlet pressure, Min (bar)",
    "Gear oil inlet pressure, StdDev (bar)",
    "Gear oil pump pressure, Max (bar)",
    "Gear oil pump pressure, Min (bar)",
    "Gear oil pump pressure, StdDev (bar)",
    "Grid frequency (Hz)",
    "Grid frequency, Max (Hz)",
    "Grid frequency, Min (Hz)",
    "Grid frequency, Standard deviation (Hz)",
    "Equivalent Full Load Hours (s)",
    "Equivalent Full Load Hours counter (s)",
    "Production Factor",
    "Performance Index",
    "Apparent power (kVA)",
    "Apparent power, Max (kVA)",
    "Apparent power, Min (kVA)",
    "Apparent power, StdDev (kVA)",
    "Cable windings from calibration point",
    "Metal particle count",
    "Metal particle count counter",
    "Cable windings from calibration point, Max",
    "Cable windings from calibration point, Min",
    "Cable windings from calibration point, StdDev",
    "Drive train acceleration (mm/ss)",
    "Tower Acceleration X (mm/ss)",
    "Tower Acceleration y (mm/ss)",
    "Tower Acceleration X, Min (mm/ss)",
    "Tower Acceleration X, Max (mm/ss)",
    "Tower Acceleration Y, Min (mm/ss)",
    "Tower Acceleration Y, Max (mm/ss)",
    "Drive train acceleration, Max (mm/ss)",
    "Drive train acceleration, Min (mm/ss)",
    "Drive train acceleration, StdDev (mm/ss)",
    "Tower Acceleration X, StdDev (mm/ss)",
    "Tower Acceleration Y, StdDev (mm/ss)",
]


def find_files_for_turbine(turbine_str: str, data_dir: Path) -> List[Path]:
    """Find files for a given turbine string (handles '1' and '01').

    Only match files that start with the exact prefix 'Turbine_Data_Penmanshiel_{id}_'.
    """
    patterns = [f"Turbine_Data_Penmanshiel_{turbine_str}_*.csv", f"Turbine_Data_Penmanshiel_{int(turbine_str)}_*.csv"]
    files = []
    for p in patterns:
        files.extend(sorted(data_dir.glob(p)))
    # dedupe and ensure we only keep files that start with Turbine_Data_Penmanshiel_{...}_
    unique = []
    seen = set()
    for f in files:
        name = f.name
        if not (name.startswith(f"Turbine_Data_Penmanshiel_{turbine_str}_") or name.startswith(f"Turbine_Data_Penmanshiel_{int(turbine_str)}_")):
            continue
        if str(f) not in seen:
            unique.append(f)
            seen.add(str(f))
    if unique:
        print(f"Found {len(unique)} Turbine_Data files for turbine {turbine_str}:")
        for u in unique:
            print("  ", u.name)
    return unique


def read_csv_flexible(path: Path) -> pd.DataFrame:
    """Read a CSV with encoding and engine fallbacks, skipping commented header lines."""
    # try encodings and engines (prefer C engine for speed, fallback to python engine)
    read_common = dict(na_values=['-'])
    last_exc = None

    # First, sample a few rows with header=None to inspect whether the first column looks like a datetime
    for enc in ("utf-8", "cp1252"):
        for engine in ("c", "python"):
            try:
                sample = pd.read_csv(path, encoding=enc, engine=engine, header=None, comment='#', skipinitialspace=True, low_memory=False, nrows=5, **read_common)
                if sample.shape[1] > 0:
                    first_val = str(sample.iat[0, 0])
                    # loosened datetime regex: YYYY-??-?? or startswith 4digit-year
                    if re.match(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2})?", first_val):
                        # treat as headerless data file
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
                # sampling failed for this encoding/engine combo; try next
                last_exc = None

    # If we reach here, try reading with header rows (common patterns header=8 then header=0)
    for enc in ("utf-8", "cp1252"):
        for engine in ("c", "python"):
            try:
                df = pd.read_csv(path, encoding=enc, engine=engine, header=8, comment='#', skipinitialspace=True, low_memory=False, **read_common)
                # if header row produced numeric-like column names (data), fall back to header=0
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
    """Select and order columns matching target_cols.

    Matching strategy:
    - case-insensitive exact match
    - if not found, match by normalizing (remove spaces and punctuation)
    - if still not found, try keyword intersection (all words in target appear in column name)
    - missing targets become columns filled with pd.NA
    """
    cols = list(df.columns)
    normalized_map = {}
    for c in cols:
        norm = re.sub(r"\W+", "", c).lower()
        normalized_map[norm] = c

    selected_series = []
    for tc in target_cols:
        # try exact case-insensitive
        found = None
        for c in cols:
            if c.strip().lower() == tc.strip().lower():
                found = c
                break
        if not found:
            # normalized match
            norm_tc = re.sub(r"\W+", "", tc).lower()
            if norm_tc in normalized_map:
                found = normalized_map[norm_tc]
        if not found:
            # keyword match: all words of target appear in candidate column
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
    # ensure column order
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


def write_to_sqlite(df: pd.DataFrame, out_db: Path, table_name: str) -> None:
    out_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(out_db))
    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        # create index on any timestamp-like column if present
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


def main() -> int:
    # command-line handling: allow processing a single turbine id or 'all'
    import sys

    def process_turbine_id(tid: str) -> int:
        # discover files for this turbine (accept leading zero and non-leading)
        files = []
        for t in (tid, str(int(tid))):
            files.extend(find_files_for_turbine(t, DATA_DIR))
        files = sorted(set(files), key=lambda p: str(p))
        if not files:
            print(f"No turbine data files found for turbine {tid} in {DATA_DIR}")
            return 1
        dfs = read_all_files(files)
        if not dfs:
            print(f"No dataframes read successfully for turbine {tid}. Skipping.")
            return 1
        # debug: show columns detected in the first file to confirm header parsing
        first_cols = list(dfs[0].columns)
        print(f"Detected columns in first CSV for turbine {tid} (sample):")
        for c in first_cols[:10]:
            print(repr(c)[:120])
        all_df = concat_and_dedup(dfs)
        if all_df.empty:
            print(f"No data to write for turbine {tid} after concat/dedup. Skipping.")
            return 1
        prepared = select_and_order_columns(all_df, TARGET_COLUMNS)
        print(f"Selected {len(prepared.columns)} columns for DB for turbine {tid} (in order).")
        out_db = OUT_DIR / f"penmanshiel_{tid}_data.db"
        write_to_sqlite(prepared, out_db, OUT_TABLE)
        total, cols, rows = verify_db(out_db, OUT_TABLE, sample=3)
        print(f"Wrote {total} rows to {out_db} table '{OUT_TABLE}'")
        return 0

    args = sys.argv[1:] if len(sys.argv) > 1 else []
    if not args:
        # default behavior: process turbine 01 only (preserve original behaviour)
        return process_turbine_id('01')
    if args[0].lower() == 'all':
        exit_codes = []
        for i in range(1, 16):
            tid = f"{i:02d}"
            print("\n=== Processing turbine", tid, "===")
            rc = process_turbine_id(tid)
            exit_codes.append(rc)
        # return non-zero if any failed
        return 0 if all(c == 0 for c in exit_codes) else 2
    # otherwise treat args as specific turbine ids
    exit_codes = []
    for a in args:
        tid = a.zfill(2)
        rc = process_turbine_id(tid)
        exit_codes.append(rc)
    return 0 if all(c == 0 for c in exit_codes) else 2


if __name__ == '__main__':
    raise SystemExit(main())
