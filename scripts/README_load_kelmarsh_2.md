Loader: `scripts/load_kelmarsh_2_to_sqlite.py`

Purpose:
- Import all CSV files with names matching `Turbine_Data_Kelmarsh_2_*.csv` from `data/kelmarsh_data` into a sqlite DB at `data/sqlitedbs/kelmarsh_2_data.db` (default).

Usage (from project root):

python scripts\load_kelmarsh_2_to_sqlite.py \
  --input-dir data/kelmarsh_data \
  --pattern "Turbine_Data_Kelmarsh_2_*.csv" \
  --db-path data/sqlitedbs/kelmarsh_2_data.db \
  --table kelmarsh_2

Notes:
- Script auto-detects the header line that starts with `# Date and time,...` and parses it correctly (handles quoted column names).
- Timestamps in the detected `Date and time` column are parsed to UTC ISO strings.
- The script uses `INSERT OR IGNORE` to avoid inserting duplicate rows; a UNIQUE index is created on the timestamp column if detected.

Requirements:
- Python 3.8+
- pandas (install with `pip install -r scripts/requirements.txt`)

Do not run the script in the current environment via the assistant; run locally where your data resides.

