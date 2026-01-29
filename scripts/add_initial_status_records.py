"""
Script to add initial records for each turbine in kelmarsh_status_by_turbine.db.

For each turbine:
- Add a record with Timestamp start = 2016-06-08 00:00:00
- Set Timestamp end = Timestamp start of the first existing record
- Calculate Duration = Timestamp end - Timestamp start
- Set Message = "Initial record, next message in HH:MM:SS"
"""

import sqlite3
from datetime import datetime, timedelta

DB_PATH = r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\kelmarsh_status_by_turbine.db"
INITIAL_TIMESTAMP = "2016-06-08 00:00:00"


def parse_timestamp(ts_str):
    """Parse a timestamp string to datetime object."""
    if not ts_str or ts_str.strip() == '':
        return None
    try:
        return datetime.strptime(ts_str.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def format_duration(td):
    """Format a timedelta as HH:MM:SS string."""
    total_seconds = int(td.total_seconds())
    if total_seconds < 0:
        return "00:00:00"
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def get_column_names(cursor, table_name):
    """Get column names for a table."""
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    return [row[1] for row in cursor.fetchall()]


def process_turbine(conn, table_name, turbine_num):
    """Add initial record for a single turbine table."""
    cursor = conn.cursor()

    # Get column names
    columns = get_column_names(cursor, table_name)
    print(f"  Columns: {columns}")

    # Get the first record (lowest rowid) to determine the first timestamp
    cursor.execute(f"""
        SELECT [Timestamp start] FROM [{table_name}]
        ORDER BY rowid ASC
        LIMIT 1
    """)
    first_row = cursor.fetchone()

    if not first_row:
        print(f"  No records found in {table_name}, skipping...")
        return False

    first_ts_start = first_row[0]
    print(f"  First existing timestamp: {first_ts_start}")

    # Check if initial record already exists
    cursor.execute(f"""
        SELECT COUNT(*) FROM [{table_name}]
        WHERE [Timestamp start] = ?
    """, (INITIAL_TIMESTAMP,))

    if cursor.fetchone()[0] > 0:
        print(f"  Initial record already exists, skipping...")
        return False

    # Parse timestamps
    initial_dt = parse_timestamp(INITIAL_TIMESTAMP)
    first_dt = parse_timestamp(first_ts_start)

    if not initial_dt or not first_dt:
        print(f"  Could not parse timestamps, skipping...")
        return False

    # Calculate duration
    duration = first_dt - initial_dt
    duration_str = format_duration(duration)

    # Format message
    message = f"Initial record, next message in {duration_str}"

    print(f"  Timestamp end: {first_ts_start}")
    print(f"  Duration: {duration_str}")
    print(f"  Message: {message}")

    # Build the INSERT statement based on available columns
    # Common columns in status tables
    insert_data = {
        'Timestamp start': INITIAL_TIMESTAMP,
        'Timestamp end': first_ts_start,
        'Duration': duration_str,
        'Status category': 'Informational',
        'Code': '0',
        'Message': message,
        'Comment': None,
        'Service contract category': None,
        'IEC category': None,
        'Global contract category': None,
        'Turbine': turbine_num
    }

    # Filter to only include columns that exist in the table
    available_data = {k: v for k, v in insert_data.items() if k in columns}

    # Build INSERT statement
    col_names = ', '.join([f'[{k}]' for k in available_data.keys()])
    placeholders = ', '.join(['?' for _ in available_data])
    values = list(available_data.values())

    # Insert at the beginning by using a temporary approach:
    # We need to insert with a rowid that comes before the first existing rowid
    # First, get the minimum rowid
    cursor.execute(f"SELECT MIN(rowid) FROM [{table_name}]")
    min_rowid = cursor.fetchone()[0]

    if min_rowid and min_rowid > 1:
        # Insert with rowid = min_rowid - 1 if possible
        new_rowid = min_rowid - 1
        sql = f"INSERT INTO [{table_name}] (rowid, {col_names}) VALUES (?, {placeholders})"
        cursor.execute(sql, [new_rowid] + values)
    else:
        # Need to shift all existing rowids up and insert at rowid 1
        # This is more complex - let's just insert normally and it will be at the end
        # But we want it at the beginning, so we need a different approach

        # Create a temp table, insert our new record first, then copy all old records
        print("  Reorganizing table to insert initial record at the beginning...")

        # Get all existing records
        cursor.execute(f"SELECT * FROM [{table_name}] ORDER BY rowid ASC")
        all_rows = cursor.fetchall()

        # Get column info for recreation
        cursor.execute(f"PRAGMA table_info([{table_name}])")
        col_info = cursor.fetchall()
        col_defs = ', '.join([f'[{c[1]}] {c[2]}' for c in col_info])

        # Drop and recreate table
        cursor.execute(f"DROP TABLE [{table_name}]")
        cursor.execute(f"CREATE TABLE [{table_name}] ({col_defs})")

        # Insert initial record first
        sql = f"INSERT INTO [{table_name}] ({col_names}) VALUES ({placeholders})"
        cursor.execute(sql, values)

        # Re-insert all existing records
        all_cols = ', '.join([f'[{c[1]}]' for c in col_info])
        all_placeholders = ', '.join(['?' for _ in col_info])
        for row in all_rows:
            cursor.execute(f"INSERT INTO [{table_name}] ({all_cols}) VALUES ({all_placeholders})", row)

    conn.commit()
    return True


def main():
    conn = sqlite3.connect(DB_PATH)

    print(f"Connected to: {DB_PATH}")
    print(f"Initial timestamp: {INITIAL_TIMESTAMP}")
    print("=" * 60)
    print("Adding initial records for each turbine...")
    print()

    success_count = 0

    for turbine in range(1, 7):
        table_name = f"turbine_{turbine}"
        print(f"{table_name}:")

        if process_turbine(conn, table_name, turbine):
            success_count += 1
            print(f"  ✓ Initial record added")
        print()

    # Vacuum to optimize
    print("Running VACUUM...")
    conn.execute("VACUUM")

    conn.close()

    print("=" * 60)
    print(f"Successfully added initial records to {success_count} turbine tables")
    print("Done!")


if __name__ == "__main__":
    main()

