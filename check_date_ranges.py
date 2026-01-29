import sqlite3
from datetime import datetime

DATA_DBS = {
    'kelmarsh': r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\kelmarsh_data_by_turbine.db",
    'penmanshiel': r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\penmanshiel_data_by_turbine.db"
}

for site, db_path in DATA_DBS.items():
    print(f"=== {site.upper()} Data Date Range ===")
    conn = sqlite3.connect(db_path)

    # Get first table
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name LIMIT 1")
    table = cur.fetchone()[0]

    # Get column names
    cur = conn.execute(f"PRAGMA table_info([{table}])")
    columns = [row[1] for row in cur.fetchall()]
    print(f"Columns: {columns}")

    # Find date column
    date_col = None
    for col in columns:
        if 'date' in col.lower() or 'time' in col.lower() or 'timestamp' in col.lower():
            date_col = col
            break

    if date_col:
        print(f"Date column: {date_col}")

        # Get min and max dates
        cur = conn.execute(f"SELECT MIN([{date_col}]), MAX([{date_col}]) FROM [{table}]")
        min_date, max_date = cur.fetchone()
        print(f"Date range: {min_date} to {max_date}")

        # Get distinct dates (just first 10 as sample)
        cur = conn.execute(f"SELECT DISTINCT DATE([{date_col}]) as d FROM [{table}] ORDER BY d LIMIT 10")
        dates = [row[0] for row in cur.fetchall()]
        print(f"Sample dates: {dates}")

        # Count distinct dates
        cur = conn.execute(f"SELECT COUNT(DISTINCT DATE([{date_col}])) FROM [{table}]")
        count = cur.fetchone()[0]
        print(f"Total distinct dates: {count}")

    conn.close()
    print()

