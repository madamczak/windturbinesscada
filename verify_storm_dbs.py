import sqlite3
from pathlib import Path

DB_DIR = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\dennisciara")

for db_file in DB_DIR.glob("*.db"):
    print(f"\n{'='*50}")
    print(f"Database: {db_file.name}")
    size_mb = db_file.stat().st_size / (1024 * 1024)
    print(f"Size: {size_mb:.2f} MB")

    conn = sqlite3.connect(str(db_file))

    # Get tables
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cur.fetchall()]
    print(f"Tables: {len(tables)}")

    for table in tables:
        cur = conn.execute(f"SELECT COUNT(*) FROM [{table}]")
        count = cur.fetchone()[0]

        # Get date range
        cur = conn.execute(f"PRAGMA table_info([{table}])")
        cols = [row[1] for row in cur.fetchall()]
        date_col = next((c for c in cols if 'date' in c.lower() or 'time' in c.lower()), None)

        if date_col:
            cur = conn.execute(f"SELECT MIN([{date_col}]), MAX([{date_col}]) FROM [{table}]")
            min_date, max_date = cur.fetchone()
            print(f"  {table}: {count} records ({min_date} to {max_date})")
        else:
            print(f"  {table}: {count} records")

    conn.close()

print("\n" + "="*50)
print("Done!")

