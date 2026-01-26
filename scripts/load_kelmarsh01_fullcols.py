"""
Load Kelmarsh turbine 1 CSVs into a SQLite DB using the full provided header.

Behavior:
- Finds files matching Turbine_Data_Kelmarsh_1_*.csv in data/kelmarsh_data
- Reads each file sequentially, streaming in chunks to limit memory
- Uses flexible parsing (detects headerless or header at row 8), but ultimately selects and orders
  columns to match the provided TARGET_COLUMNS list so the DB columns match your header
- Writes to data/sqlitedbs/kelmarsh_1_data.db table "Kelmarsh Data"

Usage: python scripts/load_kelmarsh01_fullcols.py
"""
from pathlib import Path
import sqlite3
from typing import List
import re
import argparse

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
TURBINE_ID = "1"
CHUNKSIZE = 100_000

# Full TARGET_COLUMNS taken from your request (exact order preserved)
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
    "Energy Budget (weather adjusted) (kWh)",
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
    "Potential Power Energy Budget (kW)",
    "Turbine Power setpoint, Max (kW)",
    "Turbine Power setpoint, Min (kW)",
    "Turbine Power setpoint, StdDev (kW)",
    "Available Capacity for Production (kW)",
    "Available Capacity for Production (Planned) (kW)",
    "Manufacturer Potential Power (SCADA) (kW)",
    "Available Capacity for Production (Planning deviation) (kW)",
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
    "Sunset Delta (s)",
    "Sunrise Delta (s)",
    "Available Capacity for Production (Planning deviation) (%)",
    "Time-based System Availability (Planning deviation)",
    "Production Factor",
    "Performance Index",
    "Investment Performance Ratio",
    "Operating Performance Ratio",
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
    "Drive train acceleration (mm/s2)",
    "Tower Acceleration X (mm/s2)",
    "Tower Acceleration y (mm/s2)",
    "Tower Acceleration X, Min (mm/s2)",
    "Tower Acceleration X, Max (mm/s2)",
    "Tower Acceleration Y, Min (mm/s2)",
    "Tower Acceleration Y, Max (mm/s2)",
    "Drive train acceleration, Max (mm/s2)",
    "Drive train acceleration, Min (mm/s2)",
    "Drive train acceleration, StdDev (mm/s2)",
    "Tower Acceleration X, StdDev (mm/s2)",
    "Tower Acceleration Y, StdDev (mm/s2)",
    "Night Time",
    "MTBF (Contractual Global) (h)",
    "MTTR (Contractual Global) (h)",
]


def find_files_for_turbine(tid: str, data_dir: Path) -> List[Path]:
    patterns = [f"Turbine_Data_Kelmarsh_{tid}_*.csv", f"Turbine_Data_Kelmarsh_{int(tid)}_*.csv"]
    files = []
    for p in patterns:
        files.extend(sorted(data_dir.glob(p)))
    unique = []
    seen = set()
    for f in files:
        name = f.name
        if not name.startswith(f"Turbine_Data_Kelmarsh_{tid}_") and not name.startswith(f"Turbine_Data_Kelmarsh_{int(tid)}_"):
            continue
        if str(f) not in seen:
            unique.append(f)
            seen.add(str(f))
    return unique


def select_and_order_columns(df: pd.DataFrame, target_cols: List[str]) -> pd.DataFrame:
    cols = list(df.columns)
    normalized_map = {re.sub(r"\W+", "", c).lower(): c for c in cols}
    series = []
    for tc in target_cols:
        found = None
        for c in cols:
            if c.strip().lower() == tc.strip().lower():
                found = c
                break
        if not found:
            norm = re.sub(r"\W+", "", tc).lower()
            if norm in normalized_map:
                found = normalized_map[norm]
        if found:
            series.append(df[found].rename(tc))
        else:
            series.append(pd.Series([pd.NA] * len(df), name=tc))
    out = pd.concat(series, axis=1)
    out = out[target_cols]
    return out


def write_chunk_to_sqlite(df: pd.DataFrame, out_db: Path, table: str, if_exists: str):
    out_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(out_db))
    try:
        df.to_sql(table, conn, if_exists=if_exists, index=False)
    finally:
        conn.close()


def process_file(path: Path, out_db: Path, table: str, first_file: bool) -> int:
    read_common = dict(na_values=['-'], comment='#', skipinitialspace=True)
    rows_written = 0
    # Prefer header=8 (9th row) because CSVs contain metadata in the first 8 rows
    # Fall back to autodetect if header=8 fails for a file
    # Try encodings in order
    for enc in ("utf-8", "cp1252"):
        # First, optimistic path: attempt header=8 where pandas will read the 9th row as header
        try:
            # Try a small read to ensure header=8 produces usable columns
            hdr = pd.read_csv(path, encoding=enc, engine='c', header=8, nrows=1, **read_common)
            # If columns look like data (start with digits) then header=8 probably failed; treat as failure
            if any(re.match(r"^\d", str(c).strip()) for c in hdr.columns[:3]):
                raise ValueError("header=8 produced numeric-like column names; will try fallback")
            header_mode = 8
            names = None
        except Exception:
            # fallback: try to detect headerless data (first column looks like datetime) as in previous logic
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
                # determine number of columns
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
                # last fallback: header at row 8 or header=0
                header_mode = 8
                names = None
        # Now attempt to stream the file in chunks using the decided header_mode
        try:
            reader = pd.read_csv(path, encoding=enc, engine='c', header=header_mode, names=names, chunksize=CHUNKSIZE, low_memory=True, **read_common)
        except Exception:
            try:
                reader = pd.read_csv(path, encoding=enc, engine='python', header=header_mode, names=names, chunksize=CHUNKSIZE, low_memory=True, **read_common)
            except Exception:
                continue
        first_chunk = first_file
        for chunk in reader:
            chunk.columns = [str(c).strip() for c in chunk.columns]
            prepared = select_and_order_columns(chunk, TARGET_COLUMNS)
            if prepared.empty:
                continue
            write_chunk_to_sqlite(prepared, out_db, table, 'replace' if first_chunk else 'append')
            rows_written += len(prepared)
            first_chunk = False
        return rows_written
    raise RuntimeError(f"Failed to read CSV {path} with available encodings")


def main() -> int:
    parser = argparse.ArgumentParser(description='Load Kelmarsh turbine 1 CSVs into sqlite DB with full header.')
    parser.add_argument('--single-file', '-f', help='Process only this single CSV file (path)')
    parser.add_argument('--chunksize', '-c', type=int, default=CHUNKSIZE, help='Chunk size for streaming')
    parser.add_argument('--recreate', action='store_true', help='If set, remove existing output DB before processing')
    args = parser.parse_args()

    chunksize = args.chunksize

    # update global CHUNKSIZE used by process_file via closure
    global CHUNKSIZE
    CHUNKSIZE = chunksize

    if args.recreate and OUT_DB.exists():
        print(f"Removing existing DB {OUT_DB}")
        OUT_DB.unlink()

    if args.single_file:
        fpath = Path(args.single_file)
        if not fpath.exists():
            print(f"Specified file does not exist: {fpath}")
            return 1
        print(f"Processing single file: {fpath.name}")
        # decide if this is first write (DB missing) or append
        first = not OUT_DB.exists()
        try:
            written = process_file(fpath, OUT_DB, OUT_TABLE, first_file=first)
        except Exception as e:
            print(f"Failed to process {fpath}: {e}")
            return 1
        print(f"Wrote {written} rows from {fpath.name} (mode={'replace' if first else 'append'})")
    else:
        files = find_files_for_turbine(TURBINE_ID, DATA_DIR)
        files = sorted(files, key=lambda p: str(p))
        if not files:
            print(f"No files found for turbine {TURBINE_ID} in {DATA_DIR}")
            return 1
        print(f"Found {len(files)} files for Turbine {TURBINE_ID}:")
        for f in files:
            print("  ", f.name)
        first = not OUT_DB.exists()
        total = 0
        for f in files:
            print(f"\nProcessing {f.name} ...")
            try:
                written = process_file(f, OUT_DB, OUT_TABLE, first_file=first)
            except Exception as e:
                print(f"Failed to process {f.name}: {e}")
                continue
            print(f"Wrote {written} rows from {f.name} (mode={'replace' if first else 'append'})")
            total += written
            first = False
    # verify
    conn = sqlite3.connect(str(OUT_DB))
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{OUT_TABLE}"')
        total_rows = cur.fetchone()[0]
        cur.execute(f'PRAGMA table_info("{OUT_TABLE}")')
        cols = [r[1] for r in cur.fetchall()]
        print(f"\nFinal DB: {OUT_DB} rows={total_rows} (expected ~{total if 'total' in locals() else 'unknown'}), columns={len(cols)}")
        print("Columns sample:", cols[:10], '...')
    finally:
        conn.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

