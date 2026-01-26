Load Kelmarsh Turbine 2 CSVs into SQLite

This script imports all files matching `Turbine_Data_Kelmarsh_2_*.csv` from the Kelmarsh data folder into a sqlite database.

Files created:
- `scripts/load_kelmarsh_turbine2.py` : main loader script
- `scripts/requirements.txt` : lists dependencies (pandas)
- `scripts/README_load_kelmarsh_turbine2.md` : this file

Basic usage (from project root):

python scripts\load_kelmarsh_turbine2.py \
  --input-dir data/kelmarsh_data \
  --pattern "Turbine_Data_Kelmarsh_2_*.csv" \
  --db-path data/sqlitedbs/kelmarsh_2.db \
  --table turbine_2

Notes:
- The script auto-detects a timestamp column if present and creates a unique index on it by default to prevent duplicate rows.
- It reads CSVs in chunks (default 10k rows) and uses `INSERT OR IGNORE` to avoid inserting duplicates.
- The script does not change the original CSVs.

