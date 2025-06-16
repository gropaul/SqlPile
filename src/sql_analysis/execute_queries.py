from typing import Tuple

import duckdb
from src.config import DATABASE_PATH
from src.sql_analysis.load_schemapile_json_to_ddb import primary_key, foreign_key, QUERIES_TABLE_NAME, \
    EXECUTABLE_QUERIES_TABLE_NAME
from src.sql_analysis.tools.sql_types import base_type_to_duckdb_type


def create_tables(repo_id: int, repo_url: str, con: duckdb.DuckDBPyConnection, sandbox_con: duckdb.DuckDBPyConnection) -> None:


    tables_with_columns = con.execute("""
        SELECT table_id, ANY_VALUE(table_name), list({
            'id': columns.id,
            'column_name': columns.column_name,
            'column_base_type': columns.column_base_type
        })
        FROM tables
        JOIN columns ON tables.id = columns.table_id
        WHERE tables.repo_id = ?
        GROUP BY table_id
    """, (repo_id,)).fetchall()

    for table_id, table_name, columns in tables_with_columns:
        sql_statement = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {
                    ',\n'.join(
                    f"{column['column_name']} {base_type_to_duckdb_type(column['column_base_type'])}"
                    for column in columns)
                }
            )
            
        """
        try:
            sandbox_con.execute(sql_statement)
        except Exception as e:
            # print(f"Error creating table {table_name} in repository {repo_id} - {repo_url}: {e}")
            continue



def execute_queries(repo_id: int, con: duckdb.DuckDBPyConnection, sandbox_con: duckdb.DuckDBPyConnection, n_sucessful_so_far:int) -> Tuple[int, int]:

    queries = con.execute(f"""
        SELECT id, sql 
        FROM queries 
        WHERE repo_id = ?
        AND type IN ('SELECT')
    """, (repo_id,)).fetchall()

    n_sucessful = 0
    n_failed = 0

    for query_id, sql in queries:
        try:
            sandbox_con.execute(sql)
            executable_id = n_sucessful_so_far + n_sucessful + 1
            insert_query = f"""
                INSERT INTO {EXECUTABLE_QUERIES_TABLE_NAME} (id, query_id)
                VALUES ({executable_id}, {query_id})
            """
            con.execute(insert_query, )
            n_sucessful += 1

        except Exception as e:
            print(f"Error executing query {query_id} in repository {repo_id}: {e}")
            n_failed += 1
            continue

    return n_sucessful, n_failed



def iterate_through_repos():

    con = duckdb.connect(DATABASE_PATH, read_only=False)

    repos = con.execute(f"""
        SELECT repos.id, repos.repo_url, COUNT(queries.id) AS query_count
        FROM repos
        JOIN queries ON repos.id = queries.repo_id
        WHERE queries.type IN ('SELECT')
        GROUP BY repos.id, repos.repo_url
        HAVING COUNT(queries.id) > 0
    """).fetchall()

    n_sucessful = 0
    n_failed = 0

    # create executable_queries table if it doesn't exist
    con.execute(f"""
        CREATE OR REPLACE TABLE {EXECUTABLE_QUERIES_TABLE_NAME} (
            id BIGINT {primary_key()},
            query_id BIGINT {foreign_key(QUERIES_TABLE_NAME, 'id')}
        )
    """)

    for repo_id, repo_url, cnt in repos:
        print(f"Processing repository {repo_id} - {repo_url}")

        sandbox_con = duckdb.connect()

        create_tables(repo_id, repo_url, con, sandbox_con)
        n_success_run, n_error_run = execute_queries(repo_id, con, sandbox_con, n_sucessful)

        n_sucessful += n_success_run
        n_failed += n_error_run

        sandbox_con.close()
        print(f"Executed {n_success_run}/{n_success_run+n_error_run} queries in repository {repo_id} - {repo_url}")
        print(f"Total successful queries: {n_sucessful}, Total failed queries: {n_failed}")




if __name__ == "__main__":
    iterate_through_repos()

        
        
        
        
