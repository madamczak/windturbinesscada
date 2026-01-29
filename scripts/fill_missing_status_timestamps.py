"""
Script to fill in missing 'Timestamp end' and 'Duration' for records
in kelmarsh_status_by_turbine.db.

For records where Timestamp end is NULL or empty:
- Set Timestamp end = Timestamp start of the next record (by rowid order)
- Calculate Duration = Timestamp end - Timestamp start
"""

import sqlite3
from datetime import datetime, timedelta

DB_PATH = r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\kelmarsh_status_by_turbine.db"


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


def process_turbine(conn, table_name):
    """Process a single turbine table to fill missing timestamp end and duration."""
    cursor = conn.cursor()

    # Get all records ordered by rowid
    cursor.execute(f"""
        SELECT rowid, [Timestamp start], [Timestamp end], [Duration]
        FROM [{table_name}]
        ORDER BY rowid ASC
    """)
    rows = cursor.fetchall()

    if not rows:
        print(f"  No records found in {table_name}")
        return 0

    updates = []

    for i, row in enumerate(rows):
        rowid, ts_start, ts_end, duration = row

        # Check if Timestamp end is missing (NULL or empty string)
        if ts_end is None or (isinstance(ts_end, str) and ts_end.strip() == ''):
            # Get the next record's timestamp start
            if i + 1 < len(rows):
                next_ts_start = rows[i + 1][1]

                if next_ts_start:
                    # Parse timestamps
                    start_dt = parse_timestamp(ts_start)
                    end_dt = parse_timestamp(next_ts_start)

                    if start_dt and end_dt:
                        # Calculate duration
                        dur = end_dt - start_dt
                        dur_str = format_duration(dur)

                        # Queue update
                        updates.append((next_ts_start, dur_str, rowid))

    # Apply updates
    if updates:
        cursor.executemany(f"""
            UPDATE [{table_name}]
            SET [Timestamp end] = ?, [Duration] = ?
            WHERE rowid = ?
        """, updates)
        conn.commit()

    return len(updates)


def main():
    conn = sqlite3.connect(DB_PATH)

    print(f"Connected to: {DB_PATH}")
    print("=" * 60)
    print("Filling missing Timestamp end and Duration values...")
    print()

    total_updated = 0

    for turbine in range(1, 7):
        table_name = f"turbine_{turbine}"

        # Count records with missing timestamp end before update
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*) FROM [{table_name}]
            WHERE [Timestamp end] IS NULL OR [Timestamp end] = ''
        """)
        missing_before = cursor.fetchone()[0]

        print(f"{table_name}:")
        print(f"  Records with missing Timestamp end: {missing_before}")

        # Process the table
        updated = process_turbine(conn, table_name)

        print(f"  Updated: {updated} records")
        print()

        total_updated += updated

    # Vacuum to optimize
    print("Running VACUUM...")
    conn.execute("VACUUM")

    conn.close()

    print("=" * 60)
    print(f"Total updated: {total_updated} records")
    print("Done!")


if __name__ == "__main__":
    main()

