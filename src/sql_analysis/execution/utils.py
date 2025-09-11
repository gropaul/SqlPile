import threading
from typing import Any

import duckdb

from src.config import logger

def quote(column_name: str) -> str:
    # if the column name has ` or ' in it, replace them with double quotes
    column_name = column_name.replace('`', '"').replace("'", '"')

    # if the column name is not already wrapped in quotes, wrap it in double quotes
    if not (column_name.startswith('"') and column_name.endswith('"')):
        return f'"{column_name}"'

    return column_name

def execute_with_timeout(con: duckdb.DuckDBPyConnection, sql: str, timeout: int = 10) -> Any:
    """
    Run a query with a timeout and return the result. Timeout in seconds.
    """
    def on_timeout():
        logger.error(f"Query timed out after {timeout} seconds")
        con.interrupt()

    timer = threading.Timer(timeout, on_timeout)
    timer.start()
    try:
        return con.execute(sql).fetchall()
    finally:
        timer.cancel()


def test_run_query_with_timeout():
    con = duckdb.connect(database=':memory:')
    execute_with_timeout(con, "FROM range(100)", timeout=2)  # This should timeout
    execute_with_timeout(con, "FROM range(100_000_000)", timeout=2)  # This should timeout


