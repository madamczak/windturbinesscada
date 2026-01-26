"""Run the existing loader for turbines 3..6.
This script imports `load_kelmarsh_2_to_sqlite` and calls its main() for each turbine.
It sets argv for each run so the loader processes the correct pattern and DB path.

Usage:
    python scripts\load_kelmarsh_turbines_3_6.py
"""
from __future__ import annotations
import sys
import runpy
import os

LOADER_PATH = os.path.join('scripts', 'load_kelmarsh_2_to_sqlite.py')


def run_for_turbine(turbine: int, replace_db: bool = True) -> None:
    pattern = f"Turbine_Data_Kelmarsh_{turbine}_*.csv"
    db_path = f"data/sqlitedbs/kelmarsh_{turbine}_data.db"
    table = f"kelmarsh_{turbine}"
    args = [
        'load_kelmarsh',
        '--input-dir', 'data/kelmarsh_data',
        '--pattern', pattern,
        '--db-path', db_path,
        '--table', table,
        '--batch-size', '10000',
        '--encoding', 'utf-8'
    ]
    if replace_db:
        args += ['--if-exists', 'replace']
    print(f"Running loader for turbine {turbine}: pattern={pattern} -> db={db_path}")

    # execute the loader script and call its main() if present
    globs = runpy.run_path(LOADER_PATH)
    main = globs.get('main')
    if callable(main):
        old_argv = sys.argv[:]
        try:
            sys.argv[:] = args
            main()
        finally:
            sys.argv[:] = old_argv
    else:
        # fallback: execute the script in a subprocess
        import subprocess
        cmd = ['python', LOADER_PATH] + args[1:]
        subprocess.check_call(cmd)


def main():
    for t in range(3, 7):
        try:
            run_for_turbine(turbine=t, replace_db=True)
        except Exception as e:
            print(f"Error loading turbine {t}: {e}")


if __name__ == '__main__':
    main()

