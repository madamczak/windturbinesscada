"""
Script to remove records from kelmarsh_status_by_turbine.db
where 'Timestamp start' is before 2016-06-08 00:00:00
"""

import sqlite3

DB_PATH = r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\kelmarsh_status_by_turbine.db"
CUTOFF_DATE = "2016-06-08 00:00:00"

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print(f"Connected to: {DB_PATH}")
    print(f"Cutoff date: {CUTOFF_DATE}")
    print("=" * 60)

    total_deleted = 0

    for turbine in range(1, 7):
        table_name = f"turbine_{turbine}"

        # Count records before deletion
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
        count_before = cursor.fetchone()[0]

        # Count records to be deleted
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}] WHERE [Timestamp start] < ?", (CUTOFF_DATE,))
        count_to_delete = cursor.fetchone()[0]

        # Delete records
        cursor.execute(f"DELETE FROM [{table_name}] WHERE [Timestamp start] < ?", (CUTOFF_DATE,))
        deleted = cursor.rowcount

        # Count records after deletion
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
        count_after = cursor.fetchone()[0]

        print(f"{table_name}:")
        print(f"  Before: {count_before} records")
        print(f"  Deleted: {deleted} records")
        print(f"  After: {count_after} records")
        print()

        total_deleted += deleted

    # Commit changes
    conn.commit()

    # Vacuum to reclaim space
    print("Running VACUUM to reclaim disk space...")
    conn.execute("VACUUM")

    conn.close()

    print("=" * 60)
    print(f"Total deleted: {total_deleted} records")
    print("Done!")

if __name__ == "__main__":
    main()

