import sqlite3
import sys

# Force stdout flush
sys.stdout.reconfigure(line_buffering=True)

output_file = open(r"C:\Users\adamc\PycharmProjects\windturbinesscada\windiest_days_results.txt", "w")

def log(msg):
    print(msg)
    output_file.write(msg + "\n")
    output_file.flush()

DATA_DBS = {
    'kelmarsh': r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\kelmarsh_data_by_turbine.db",
    'penmanshiel': r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\penmanshiel_data_by_turbine.db"
}

for site, db_path in DATA_DBS.items():
    log(f"\n{'='*60}")
    log(f"=== {site.upper()} - Top 20 Windiest Days ===")
    log(f"{'='*60}")

    conn = sqlite3.connect(db_path)

    # Get first table
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'turbine_%' ORDER BY name LIMIT 1")
    table = cur.fetchone()[0]

    # Get column names
    cur = conn.execute(f"PRAGMA table_info([{table}])")
    columns = [row[1] for row in cur.fetchall()]

    # Find date and wind speed columns
    date_col = None
    wind_speed_col = None

    for col in columns:
        col_lower = col.lower()
        if date_col is None and ('date' in col_lower or 'time' in col_lower or 'timestamp' in col_lower):
            date_col = col
        if wind_speed_col is None and 'wind speed' in col_lower and 'std' not in col_lower and 'min' not in col_lower and 'max' not in col_lower:
            wind_speed_col = col

    log(f"Table: {table}")
    log(f"Date column: {date_col}")
    log(f"Wind speed column: {wind_speed_col}")

    if date_col and wind_speed_col:
        # Get top 20 days by average wind speed
        query = f"""
            SELECT 
                DATE([{date_col}]) as day,
                ROUND(AVG(CAST([{wind_speed_col}] AS REAL)), 2) as avg_wind_speed,
                ROUND(MAX(CAST([{wind_speed_col}] AS REAL)), 2) as max_wind_speed,
                COUNT(*) as records
            FROM [{table}]
            WHERE [{wind_speed_col}] IS NOT NULL AND [{wind_speed_col}] != ''
            GROUP BY DATE([{date_col}])
            ORDER BY avg_wind_speed DESC
            LIMIT 20
        """

        cur = conn.execute(query)
        results = cur.fetchall()

        log(f"\n{'Date':<12} {'Avg Speed':<12} {'Max Speed':<12} {'Records':<10}")
        log("-" * 46)
        for row in results:
            log(f"{row[0]:<12} {row[1]:<12} {row[2]:<12} {row[3]:<10}")

        # Also get monthly averages
        log(f"\n--- Monthly Average Wind Speeds ---")
        query_monthly = f"""
            SELECT 
                strftime('%Y-%m', [{date_col}]) as month,
                ROUND(AVG(CAST([{wind_speed_col}] AS REAL)), 2) as avg_wind_speed
            FROM [{table}]
            WHERE [{wind_speed_col}] IS NOT NULL AND [{wind_speed_col}] != ''
            GROUP BY strftime('%Y-%m', [{date_col}])
            ORDER BY avg_wind_speed DESC
            LIMIT 10
        """

        cur = conn.execute(query_monthly)
        results = cur.fetchall()

        log(f"\n{'Month':<10} {'Avg Speed (m/s)':<15}")
        log("-" * 25)
        for row in results:
            log(f"{row[0]:<10} {row[1]:<15}")

    conn.close()

log("\n" + "="*60)
log("Done!")
output_file.close()

