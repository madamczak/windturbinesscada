#!/usr/bin/env python3
"""Compare number of columns between kelmarsh_all_data.db and penmanshiel_all_data.db.

Defaults to the databases under data/sqlitedbs but accepts two paths as command-line
arguments.
"""
import sqlite3
import sys
from pathlib import Path
from typing import Optional, Tuple, List

DEFAULT_A = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\kelmarsh_all_data.db")
DEFAULT_B = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs\penmanshiel_all_data.db")


def first_user_table(conn: sqlite3.Connection) -> Optional[str]:
    """Return the first non-sqlite_ user table name, or None if none exist."""
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 1"
    )
    row = cur.fetchone()
    return row[0] if row else None


def quote_pragma_string(name: str) -> str:
    """Return a safely escaped single-quoted string literal for use as PRAGMA argument.

    Example: name -> 'My Table'
    Escapes any single quotes inside the name by doubling them.
    """
    return "'" + name.replace("'", "''") + "'"


def get_columns(db_path: Path) -> Tuple[str, List[str]]:
    """Open the sqlite DB, find first user table and return (table_name, [col_names])."""
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    conn = sqlite3.connect(str(db_path))
    try:
        table = first_user_table(conn)
        if not table:
            raise RuntimeError(f"No user tables found in {db_path}")
        # Use a single-quoted literal for the PRAGMA argument to avoid syntax errors when
        # the name contains spaces or keywords like "Data".
        qname = quote_pragma_string(table)
        try:
            cur = conn.execute(f"PRAGMA table_info({qname})")
        except sqlite3.OperationalError as e:
            # Give a clearer error including the problematic table name
            raise RuntimeError(f"PRAGMA table_info failed for table {table!r}: {e}") from e
        cols = [r[1] for r in cur.fetchall()]
        return table, cols
    finally:
        conn.close()


def main() -> int:
    a_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_A
    b_path = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_B

    try:
        table_a, cols_a = get_columns(a_path)
        table_b, cols_b = get_columns(b_path)
    except Exception as e:
        print(f"Error: {e}")
        return 2

    len_a = len(cols_a)
    len_b = len(cols_b)
    print(f"{a_path} -> table `{table_a}`: {len_a} columns")
    print(f"{b_path} -> table `{table_b}`: {len_b} columns")
    print(f"Equal number of columns: {len_a == len_b}")

    if len_a != len_b:
        set_a = set(cols_a)
        set_b = set(cols_b)
        only_a = sorted(list(set_a - set_b))
        only_b = sorted(list(set_b - set_a))
        if only_a:
            print(f"Columns only in {a_path.name}:")
            for c in only_a:
                print("  ", c)
        if only_b:
            print(f"Columns only in {b_path.name}:")
            for c in only_b:
                print("  ", c)

    return 0 if len_a == len_b else 1


if __name__ == "__main__":
    raise SystemExit(main())
