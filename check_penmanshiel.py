import sqlite3

DATA_DB = r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\penmanshiel_data_by_turbine.db"
STATUS_DB = r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\data_by_turbine\penmanshiel_status_by_turbine.db"

print("=== Penmanshiel Data Database ===")
conn = sqlite3.connect(DATA_DB)
cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
print(f"Tables: {tables}")
for t in tables:
    cur = conn.execute(f"SELECT COUNT(*) FROM [{t}]")
    count = cur.fetchone()[0]
    print(f"  {t}: {count} records")
conn.close()

print()
print("=== Penmanshiel Status Database ===")
conn = sqlite3.connect(STATUS_DB)
cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
print(f"Tables: {tables}")
for t in tables:
    cur = conn.execute(f"SELECT COUNT(*) FROM [{t}]")
    count = cur.fetchone()[0]
    print(f"  {t}: {count} records")
conn.close()

