"""
Create SQLite databases for Storm Dennis & Storm Ciara period (February 8-22, 2020)

Storm Ciara: 8-9 February 2020
Storm Dennis: 15-16 February 2020
"""

import sqlite3
import os
from pathlib import Path

# Define paths
BASE_DIR = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs")
OUTPUT_DIR = BASE_DIR / "dennisciara"
DATA_BY_TURBINE_DIR = BASE_DIR / "data_by_turbine"

# Source databases
KELMARSH_DATA_DB = DATA_BY_TURBINE_DIR / "kelmarsh_data_by_turbine.db"
KELMARSH_STATUS_DB = DATA_BY_TURBINE_DIR / "kelmarsh_status_by_turbine.db"
PENMANSHIEL_DATA_DB = DATA_BY_TURBINE_DIR / "penmanshiel_data_by_turbine.db"
PENMANSHIEL_STATUS_DB = DATA_BY_TURBINE_DIR / "penmanshiel_status_by_turbine.db"

# Storm period
START_DATE = "2020-02-08 00:00:00"
END_DATE = "2020-02-22 23:59:59"

# Create output directory
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
print(f"Output directory: {OUTPUT_DIR}")


def get_tables(conn):
    """Get list of turbine tables."""
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'turbine_%' ORDER BY name")
    return [row[0] for row in cur.fetchall()]


def get_columns(conn, table):
    """Get column names and types for a table."""
    cur = conn.execute(f"PRAGMA table_info([{table}])")
    return [(row[1], row[2]) for row in cur.fetchall()]


def find_date_column(columns):
    """Find the date/timestamp column."""
    for col_name, col_type in columns:
        col_lower = col_name.lower()
        if 'date' in col_lower or 'time' in col_lower or 'timestamp' in col_lower:
            return col_name
    return None


def copy_table_with_filter(src_conn, dst_conn, table, date_col, start_date, end_date):
    """Copy filtered data from source to destination."""
    # Get columns
    columns = get_columns(src_conn, table)
    col_names = [c[0] for c in columns]
    col_defs = ", ".join([f"[{c[0]}] {c[1]}" for c in columns])

    # Create table in destination
    dst_conn.execute(f"CREATE TABLE IF NOT EXISTS [{table}] ({col_defs})")

    # Copy filtered data
    placeholders = ", ".join(["?" for _ in col_names])
    col_list = ", ".join([f"[{c}]" for c in col_names])

    query = f"""
        SELECT {col_list} FROM [{table}]
        WHERE [{date_col}] >= ? AND [{date_col}] <= ?
        ORDER BY rowid
    """

    src_cur = src_conn.execute(query, (start_date, end_date))
    rows = src_cur.fetchall()

    if rows:
        dst_conn.executemany(f"INSERT INTO [{table}] ({col_list}) VALUES ({placeholders})", rows)

    return len(rows)


def create_storm_db(src_path, dst_path, db_name, is_status=False):
    """Create a filtered database for the storm period."""
    print(f"\n{'='*60}")
    print(f"Creating {db_name}")
    print(f"Source: {src_path}")
    print(f"Destination: {dst_path}")
    print(f"{'='*60}")

    if not src_path.exists():
        print(f"ERROR: Source database not found: {src_path}")
        return

    # Remove existing destination if exists
    if dst_path.exists():
        dst_path.unlink()

    src_conn = sqlite3.connect(str(src_path))
    dst_conn = sqlite3.connect(str(dst_path))

    tables = get_tables(src_conn)
    print(f"Found {len(tables)} tables")

    total_records = 0

    for table in tables:
        columns = get_columns(src_conn, table)

        # Find date column - for status tables it might be "Timestamp start"
        if is_status:
            date_col = next((c[0] for c in columns if 'timestamp start' in c[0].lower()), None)
            if not date_col:
                date_col = find_date_column(columns)
        else:
            date_col = find_date_column(columns)

        if not date_col:
            print(f"  {table}: No date column found, skipping")
            continue

        count = copy_table_with_filter(src_conn, dst_conn, table, date_col, START_DATE, END_DATE)
        total_records += count
        print(f"  {table}: {count} records")

    dst_conn.commit()
    dst_conn.close()
    src_conn.close()

    # Get file size
    size_mb = dst_path.stat().st_size / (1024 * 1024)
    print(f"\nTotal: {total_records} records, {size_mb:.2f} MB")


def main():
    print("Creating Storm Dennis & Ciara databases")
    print(f"Period: {START_DATE} to {END_DATE}")
    print("="*60)

    # Create Kelmarsh data DB
    create_storm_db(
        KELMARSH_DATA_DB,
        OUTPUT_DIR / "kelmarsh_data_storms.db",
        "Kelmarsh Data (Storms)",
        is_status=False
    )

    # Create Kelmarsh status DB
    create_storm_db(
        KELMARSH_STATUS_DB,
        OUTPUT_DIR / "kelmarsh_status_storms.db",
        "Kelmarsh Status (Storms)",
        is_status=True
    )

    # Create Penmanshiel data DB
    create_storm_db(
        PENMANSHIEL_DATA_DB,
        OUTPUT_DIR / "penmanshiel_data_storms.db",
        "Penmanshiel Data (Storms)",
        is_status=False
    )

    # Create Penmanshiel status DB
    create_storm_db(
        PENMANSHIEL_STATUS_DB,
        OUTPUT_DIR / "penmanshiel_status_storms.db",
        "Penmanshiel Status (Storms)",
        is_status=True
    )

    print("\n" + "="*60)
    print("Done! Created databases in:", OUTPUT_DIR)
    print("Files created:")
    for f in OUTPUT_DIR.iterdir():
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  - {f.name}: {size_mb:.2f} MB")


if __name__ == "__main__":
    main()

