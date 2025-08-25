import threading
from typing import Any

import duckdb

from src.config import logger


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


