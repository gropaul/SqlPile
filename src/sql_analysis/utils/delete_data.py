from typing import Literal

import duckdb

from src.config import TABLES_DATA_FILES_TABLE_NAME, COLUMNS_TABLE_NAME, TABLES_TABLE_NAME, \
    QUERY_OPERATOR_COMPONENT_EXPRESSIONS, QUERY_OPERATOR_COMPONENTS_TABLE_NAME, QUERY_OPERATOR_TABLE_NAME

DeleteMode = Literal['all', 'execution_only']



def table_exists(con, table_name: str) -> bool:
    result = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE lower(table_name) = lower('{table_name}')
    """).fetchone()
    count = result[0] if result is not None else 0
    return count == 1

def delete_query_operator_component_expressions(con, repo_id):
    """Delete all component expressions linked to a given repo."""
    if table_exists(con, QUERY_OPERATOR_COMPONENT_EXPRESSIONS):
        con.execute(f"""
            DELETE FROM {QUERY_OPERATOR_COMPONENT_EXPRESSIONS}
            WHERE component_id IN (
                SELECT id FROM {QUERY_OPERATOR_COMPONENTS_TABLE_NAME}
                WHERE operator_id IN (
                    SELECT id FROM {QUERY_OPERATOR_TABLE_NAME}
                    WHERE query_id IN (
                        SELECT id FROM queries_executable
                        WHERE query_id IN (
                            SELECT id FROM queries WHERE repo_id = ?
                        )
                    )
                )
            )
        """, (repo_id,))


def delete_query_operator_components(con, repo_id):
    """Delete all operator components linked to a given repo."""
    if table_exists(con, QUERY_OPERATOR_COMPONENTS_TABLE_NAME):
        con.execute(f"""
            DELETE FROM {QUERY_OPERATOR_COMPONENTS_TABLE_NAME}
            WHERE operator_id IN (
                SELECT id FROM {QUERY_OPERATOR_TABLE_NAME}
                WHERE query_id IN (
                    SELECT id FROM queries_executable
                    WHERE query_id IN (
                        SELECT id FROM queries WHERE repo_id = ?
                    )
                )
            )
        """, (repo_id,))


def delete_query_operators(con, repo_id):
    """Delete all query operators linked to a given repo."""
    if table_exists(con, QUERY_OPERATOR_TABLE_NAME):
        con.execute(f"""
            DELETE FROM {QUERY_OPERATOR_TABLE_NAME}
            WHERE query_id IN (
                SELECT id FROM queries_executable
                WHERE query_id IN (
                    SELECT id FROM queries WHERE repo_id = ?
                )
            )
        """, (repo_id,))


def delete_repo(con: duckdb.DuckDBPyConnection, repo_id: int, mode: DeleteMode = 'all'):
    """
    Delete has two modes: 'all' and 'queries_only'.
    'all' deletes everything related to the repo, including tables and columns.
    'execution_only' only deletes data that was gathered during execution, i.e., queries and query errors.
    """
    if mode == 'all':
        if table_exists(con, "queries"):
            con.execute("DELETE FROM queries WHERE repo_id = ?", (repo_id,))

    if table_exists(con, "queries_error_select"):
        con.execute("DELETE FROM queries_error_select WHERE query_id IN (SELECT id FROM queries WHERE repo_id = ?)", (repo_id,))
        con.execute("DELETE FROM queries_error_select WHERE repo_id = ?", (repo_id,))

    if table_exists(con, "queries_error_create"):
        con.execute("DELETE FROM queries_error_create WHERE table_id IN (SELECT id FROM tables WHERE repo_id = ?)", (repo_id,))

    if table_exists(con, "queries_error_insert"):
        con.execute("DELETE FROM queries_error_insert WHERE query_id IN (SELECT id FROM queries WHERE repo_id = ?)", (repo_id,))

    if table_exists(con, "queries_error_view"):
        con.execute("DELETE FROM queries_error_view WHERE query_id IN (SELECT id FROM queries WHERE repo_id = ?)", (repo_id,))

    if table_exists(con, "queries_executable"):
        con.execute("DELETE FROM queries_executable WHERE query_id IN (SELECT id FROM queries WHERE repo_id = ?)", (repo_id,))

    if table_exists(con, "columns_compression_results"):
        con.execute("DELETE FROM columns_compression_results WHERE repo_id = ?", (repo_id,))

    delete_query_operator_component_expressions(con, repo_id)
    delete_query_operator_components(con, repo_id)
    delete_query_operators(con, repo_id)


    reset_statistics_for_repo(con, repo_id)

    # delete from column_values
    if table_exists(con, "column_values") and table_exists(con, "columns") and table_exists(con, "tables"):
        con.execute("""
            DELETE FROM column_values
            WHERE column_id IN (
                SELECT c.id FROM columns c
                JOIN tables t ON c.table_id = t.id
                WHERE t.repo_id = ?
            )
        """, (repo_id,))

    if mode == 'all':
        if table_exists(con, "columns") and table_exists(con, "tables"):
            con.execute("""
                DELETE FROM columns
                WHERE table_id IN (SELECT id FROM tables WHERE repo_id = ?)
            """, (repo_id,))

    # delte from table_values_count
    if table_exists(con, "table_values_count") and table_exists(con, "tables"):
        con.execute("""
            DELETE FROM table_values_count
            WHERE table_id IN (SELECT id FROM tables WHERE repo_id = ?)
        """, (repo_id,))

     # delete from tables

    if mode == 'all':
        if table_exists(con, "tables"):
            con.execute("DELETE FROM tables WHERE repo_id = ?", (repo_id,))

        if table_exists(con, TABLES_DATA_FILES_TABLE_NAME):
            con.execute(f"DELETE FROM {TABLES_DATA_FILES_TABLE_NAME} WHERE repo_id = ?", (repo_id,))

        if table_exists(con, "repos"):
            print("Deleting repo id", repo_id)
            con.execute("DELETE FROM repos WHERE id = ?", (repo_id,))




def reset_statistics_tables(con: duckdb.DuckDBPyConnection):
    con.execute(f"DROP TABLE IF EXISTS column_stats_text")
    con.execute(f"DROP TABLE IF EXISTS column_stats_int")
    con.execute(f"DROP TABLE IF EXISTS column_stats_float")
    con.execute(f"DROP TABLE IF EXISTS column_stats_datetime")


def reset_statistics_for_repo(
        con: duckdb.DuckDBPyConnection,
        repo_id: int
):
    print(f"Resetting statistics for repo id {repo_id}")
    if table_exists(con, "column_stats_text"):
        con.execute(f"DELETE FROM column_stats_text WHERE column_id IN (SELECT id FROM {COLUMNS_TABLE_NAME} WHERE table_id IN (SELECT id FROM {TABLES_TABLE_NAME} WHERE repo_id = {repo_id}))")

    if table_exists(con, "column_stats_int"):
        con.execute(f"DELETE FROM column_stats_int WHERE column_id IN (SELECT id FROM {COLUMNS_TABLE_NAME} WHERE table_id IN (SELECT id FROM {TABLES_TABLE_NAME} WHERE repo_id = {repo_id}))")

    if table_exists(con, "column_stats_float"):
        con.execute(f"DELETE FROM column_stats_float WHERE column_id IN (SELECT id FROM {COLUMNS_TABLE_NAME} WHERE table_id IN (SELECT id FROM {TABLES_TABLE_NAME} WHERE repo_id = {repo_id}))")

    if table_exists(con, "column_stats_datetime"):
        con.execute(f"DELETE FROM column_stats_datetime WHERE column_id IN (SELECT id FROM {COLUMNS_TABLE_NAME} WHERE table_id IN (SELECT id FROM {TABLES_TABLE_NAME} WHERE repo_id = {repo_id}))")

