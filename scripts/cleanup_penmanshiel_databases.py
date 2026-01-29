"""
Script to clean up Penmanshiel databases:
1. Remove records before 2016-06-08 00:00:00 from both data and status databases
2. Add initial records to status database
3. Fill missing timestamp end and duration values in status database
"""

import sqlite3
from datetime import datetime

# Database paths
DATA_DB_PATH = r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\penmanshiel_data_by_turbine.db"
STATUS_DB_PATH = r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\penmanshiel_status_by_turbine.db"

CUTOFF_DATE = "2016-10-07 00:00:00"
INITIAL_TIMESTAMP = "2016-10-07 00:00:00"

# Penmanshiel has turbines 1-15 (but turbine 3 may be missing in data)
TURBINE_RANGE = range(1, 16)


def parse_timestamp(ts_str):
    """Parse a timestamp string to datetime object."""
    if not ts_str or str(ts_str).strip() == '':
        return None
    ts_str = str(ts_str).strip()
    # Try different formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    return None


def format_duration(td):
    """Format a timedelta as HH:MM:SS string."""
    total_seconds = int(td.total_seconds())
    if total_seconds < 0:
        return "00:00:00"
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def get_tables(conn):
    """Get list of turbine tables in database."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'turbine_%'")
    return [row[0] for row in cursor.fetchall()]


def get_column_names(cursor, table_name):
    """Get column names for a table."""
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    return [row[1] for row in cursor.fetchall()]


def find_timestamp_column(columns):
    """Find the timestamp column name (could be 'Timestamp start', 'Date and time', etc.)."""
    for col in columns:
        col_lower = col.lower()
        if 'timestamp start' in col_lower:
            return col
        if 'date and time' in col_lower:
            return col
        if 'timestamp' in col_lower or 'datetime' in col_lower:
            return col
    return None


def delete_records_before_cutoff(conn, table_name, timestamp_col, cutoff):
    """Delete records before the cutoff date."""
    cursor = conn.cursor()

    # Count before
    cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
    count_before = cursor.fetchone()[0]

    # Count to delete
    cursor.execute(f"SELECT COUNT(*) FROM [{table_name}] WHERE [{timestamp_col}] < ?", (cutoff,))
    count_to_delete = cursor.fetchone()[0]

    # Delete
    cursor.execute(f"DELETE FROM [{table_name}] WHERE [{timestamp_col}] < ?", (cutoff,))

    # Count after
    cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
    count_after = cursor.fetchone()[0]

    conn.commit()
    return count_before, count_to_delete, count_after


def add_initial_record(conn, table_name, turbine_num, columns, timestamp_start_col, timestamp_end_col):
    """Add initial record to status table."""
    cursor = conn.cursor()

    # Check if initial record already exists
    cursor.execute(f"SELECT COUNT(*) FROM [{table_name}] WHERE [{timestamp_start_col}] = ?", (INITIAL_TIMESTAMP,))
    if cursor.fetchone()[0] > 0:
        print(f"    Initial record already exists, skipping...")
        return False

    # Get first existing record's timestamp start
    cursor.execute(f"SELECT [{timestamp_start_col}] FROM [{table_name}] ORDER BY rowid ASC LIMIT 1")
    first_row = cursor.fetchone()

    if not first_row:
        print(f"    No records found, skipping...")
        return False

    first_ts_start = first_row[0]

    # Parse timestamps
    initial_dt = parse_timestamp(INITIAL_TIMESTAMP)
    first_dt = parse_timestamp(first_ts_start)

    if not initial_dt or not first_dt:
        print(f"    Could not parse timestamps, skipping...")
        return False

    # Calculate duration
    duration = first_dt - initial_dt
    duration_str = format_duration(duration)

    # Format message
    message = f"Initial record, next message in {duration_str}"

    print(f"    First existing timestamp: {first_ts_start}")
    print(f"    Duration: {duration_str}")

    # Build insert data based on available columns
    insert_data = {}

    # Map common column names
    col_lower_map = {col.lower(): col for col in columns}

    if timestamp_start_col:
        insert_data[timestamp_start_col] = INITIAL_TIMESTAMP
    if timestamp_end_col:
        insert_data[timestamp_end_col] = first_ts_start

    # Duration
    for col in columns:
        if 'duration' in col.lower():
            insert_data[col] = duration_str
            break

    # Status/Category
    for col in columns:
        if 'status' in col.lower() or 'category' in col.lower():
            insert_data[col] = 'Informational'
            break

    # Code
    for col in columns:
        if col.lower() == 'code':
            insert_data[col] = '0'
            break

    # Message
    for col in columns:
        if 'message' in col.lower():
            insert_data[col] = message
            break

    # Turbine
    for col in columns:
        if 'turbine' in col.lower():
            insert_data[col] = turbine_num
            break

    if not insert_data:
        print(f"    No columns to insert, skipping...")
        return False

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
    col_names = ', '.join([f'[{k}]' for k in insert_data.keys()])
    placeholders = ', '.join(['?' for _ in insert_data])
    values = list(insert_data.values())
    cursor.execute(f"INSERT INTO [{table_name}] ({col_names}) VALUES ({placeholders})", values)

    # Re-insert all existing records
    all_cols = ', '.join([f'[{c[1]}]' for c in col_info])
    all_placeholders = ', '.join(['?' for _ in col_info])
    for row in all_rows:
        cursor.execute(f"INSERT INTO [{table_name}] ({all_cols}) VALUES ({all_placeholders})", row)

    conn.commit()
    return True


def fill_missing_timestamps(conn, table_name, timestamp_start_col, timestamp_end_col, duration_col):
    """Fill missing timestamp end and duration values."""
    cursor = conn.cursor()

    if not timestamp_end_col or not duration_col:
        print(f"    Missing timestamp_end or duration column, skipping fill...")
        return 0

    # Get all records
    cursor.execute(f"SELECT rowid, [{timestamp_start_col}], [{timestamp_end_col}], [{duration_col}] FROM [{table_name}] ORDER BY rowid ASC")
    rows = cursor.fetchall()

    updates = []
    for i, row in enumerate(rows):
        rowid, ts_start, ts_end, duration = row

        # Check if timestamp end is missing
        if ts_end is None or (isinstance(ts_end, str) and ts_end.strip() == ''):
            if i + 1 < len(rows):
                next_ts_start = rows[i + 1][1]
                if next_ts_start:
                    start_dt = parse_timestamp(ts_start)
                    end_dt = parse_timestamp(next_ts_start)
                    if start_dt and end_dt:
                        dur = end_dt - start_dt
                        dur_str = format_duration(dur)
                        updates.append((next_ts_start, dur_str, rowid))

    # Apply updates
    if updates:
        cursor.executemany(f"UPDATE [{table_name}] SET [{timestamp_end_col}] = ?, [{duration_col}] = ? WHERE rowid = ?", updates)
        conn.commit()

    return len(updates)


def process_data_database():
    """Process the Penmanshiel data database."""
    print("=" * 70)
    print("Processing Penmanshiel DATA database")
    print("=" * 70)
    print(f"Path: {DATA_DB_PATH}")
    print(f"Cutoff: {CUTOFF_DATE}")
    print()

    conn = sqlite3.connect(DATA_DB_PATH)
    tables = get_tables(conn)
    print(f"Found tables: {tables}")
    print()

    total_deleted = 0

    for table_name in sorted(tables):
        print(f"{table_name}:")
        cursor = conn.cursor()
        columns = get_column_names(cursor, table_name)
        ts_col = find_timestamp_column(columns)

        if not ts_col:
            print(f"  No timestamp column found, skipping...")
            continue

        print(f"  Timestamp column: {ts_col}")

        before, deleted, after = delete_records_before_cutoff(conn, table_name, ts_col, CUTOFF_DATE)
        print(f"  Before: {before}, Deleted: {deleted}, After: {after}")
        total_deleted += deleted
        print()

    conn.close()

    print(f"Total deleted from DATA: {total_deleted}")
    print()


def process_status_database():
    """Process the Penmanshiel status database."""
    print("=" * 70)
    print("Processing Penmanshiel STATUS database")
    print("=" * 70)
    print(f"Path: {STATUS_DB_PATH}")
    print(f"Cutoff: {CUTOFF_DATE}")
    print()

    conn = sqlite3.connect(STATUS_DB_PATH)
    tables = get_tables(conn)
    print(f"Found tables: {tables}")
    print()

    total_deleted = 0
    total_initial = 0
    total_filled = 0

    for table_name in sorted(tables):
        print(f"{table_name}:")
        cursor = conn.cursor()
        columns = get_column_names(cursor, table_name)
        print(f"  Columns: {columns}")

        # Find relevant columns
        ts_start_col = None
        ts_end_col = None
        duration_col = None

        for col in columns:
            col_lower = col.lower()
            if 'timestamp start' in col_lower:
                ts_start_col = col
            elif 'timestamp end' in col_lower:
                ts_end_col = col
            elif col_lower == 'duration':
                duration_col = col

        if not ts_start_col:
            ts_start_col = find_timestamp_column(columns)

        if not ts_start_col:
            print(f"  No timestamp column found, skipping...")
            continue

        print(f"  Timestamp start: {ts_start_col}")
        print(f"  Timestamp end: {ts_end_col}")
        print(f"  Duration: {duration_col}")

        # Step 1: Delete old records
        before, deleted, after = delete_records_before_cutoff(conn, table_name, ts_start_col, CUTOFF_DATE)
        print(f"  Deleted: {deleted} records (Before: {before}, After: {after})")
        total_deleted += deleted

        # Step 2: Fill missing timestamps and durations
        if ts_end_col and duration_col:
            filled = fill_missing_timestamps(conn, table_name, ts_start_col, ts_end_col, duration_col)
            print(f"  Filled: {filled} records with missing timestamp end/duration")
            total_filled += filled

        # Step 3: Add initial record
        # Extract turbine number from table name
        turbine_num = int(table_name.replace("turbine_", ""))
        print(f"  Adding initial record for turbine {turbine_num}...")
        if add_initial_record(conn, table_name, turbine_num, columns, ts_start_col, ts_end_col):
            total_initial += 1
            print(f"  ✓ Initial record added")

        print()

    conn.close()

    print(f"Total deleted from STATUS: {total_deleted}")
    print(f"Total initial records added: {total_initial}")
    print(f"Total records with filled timestamps: {total_filled}")
    print()


def main():
    print("Penmanshiel Database Cleanup Script")
    print("=" * 70)
    print()

    # Process data database
    process_data_database()

    # Process status database
    process_status_database()

    print("=" * 70)
    print("Done!")


if __name__ == "__main__":
    main()

