from typing import Tuple
import os
import pandas as pd
import duckdb
from tqdm import tqdm

from src.config import DATABASE_PATH
from src.sql_analysis.execution.mock_query import MockQueryResult, try_to_mock_and_execute_query
from src.sql_analysis.execution.models import Table, Column
from src.sql_analysis.execution.prepare_sql_for_execution import prepare_sql_for_duckdb
from src.sql_analysis.load_schemapile_json_to_ddb import primary_key, foreign_key, QUERIES_TABLE_NAME, \
    EXECUTABLE_QUERIES_TABLE_NAME
from src.sql_analysis.tools.sql_types import base_type_to_duckdb_type
from src.sql_analysis.execution.extra_functions import EXTRA_FUNCTIONS

import re

# Create a DataFrame to store errors
error_df = pd.DataFrame(columns=['error_type', 'repo_id', 'repo_url', 'query_id', 'table_name', 'error_message', 'sql'])

n_failed_table_creations = 0
n_successful_table_creations = 0

from typing import List, Literal
from itertools import product



def create_tables(repo_id: int, repo_url: str, con: duckdb.DuckDBPyConnection, sandbox_con: duckdb.DuckDBPyConnection) -> List[Table]:

    tables_with_columns = con.execute("""
        SELECT table_id, ANY_VALUE(table_name), list_distinct(list({
            'id': columns.id,
            'column_name': columns.column_name,
            'column_base_type': columns.column_base_type
        }))
        FROM tables
        JOIN columns ON tables.id = columns.table_id
        WHERE tables.repo_id = ?
        GROUP BY table_id
    """, (repo_id,)).fetchall()

    tables = []

    for table_id, table_name, columns in tables_with_columns:

        def quote_name(column_name: str) -> str:
            # if the column name has ` or ' in it, replace them with double quotes
            column_name = column_name.replace('`', '"').replace("'", '"')

            # if the column name is not already wrapped in quotes, wrap it in double quotes
            if not (column_name.startswith('"') and column_name.endswith('"')):
                return f'"{column_name}"'

            return column_name

        # check if the table has a schema like `schema.table`
        if '.' in table_name:
            # if it does, create the schema if it doesn't exist
            schema_name, name_without_schema = table_name.split('.', 1)

            complete_quoted_table_name = f'{quote_name(schema_name)}.{quote_name(name_without_schema)}'
            sandbox_con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_name(schema_name)}")

        else:
            complete_quoted_table_name = quote_name(table_name)

        try:
            sql_statement = f"""
                 CREATE TABLE IF NOT EXISTS {complete_quoted_table_name} ({
            ',\n'.join(
                f'{quote_name(column['column_name'])} {base_type_to_duckdb_type(column['column_base_type'])}'
                for column in columns)
            })"""
            sql_statement = prepare_sql_for_duckdb(sql_statement)
            sandbox_con.execute(sql_statement)
            table = Table(table_id, complete_quoted_table_name,
                          [Column(column['id'], column['column_name'], column['column_base_type']) for column in
                           columns])
            tables.append(table)
            global n_successful_table_creations
            n_successful_table_creations += 1
        except Exception as e: #`trivia_user_cache`
            print(f"Error creating table {complete_quoted_table_name} in repository {repo_id} - {repo_url}: {e}")
            print(f"SQL Statement: {sql_statement}")
            # Add error to DataFrame
            global error_df
            error_df = pd.concat([error_df, pd.DataFrame([{
                'error_type': 'table_creation',
                'repo_id': repo_id,
                'repo_url': repo_url,
                'query_id': None,
                'table_name': complete_quoted_table_name,
                'error_message': str(e),
                'sql': sql_statement
            }])], ignore_index=True)

            global n_failed_table_creations
            n_failed_table_creations += 1
            continue

    print(f"Created {len(tables)} tables in repository {repo_id} - {repo_url}")
    return tables


def escape_sql(sql: str) -> str:
    # Escape single quotes by replacing them with two single quotes
    return sql.replace("'", "''")

def execute_queries(repo_id: int, repo_url: str, con: duckdb.DuckDBPyConnection, sandbox_con: duckdb.DuckDBPyConnection, n_sucessful_so_far:int, tables: List[Table]) -> Tuple[int, int]:

    queries = con.execute(f"""
        SELECT id, sql 
        FROM queries 
        WHERE repo_id = ?
        AND type IN ('SELECT')
    """, (repo_id,)).fetchall()

    n_successful = 0
    n_failed = 0

    for query_id, sql in queries:
        sql_prepared = prepare_sql_for_duckdb(sql)
        result: MockQueryResult = try_to_mock_and_execute_query(sql_prepared, sandbox_con, tables)

        if result.was_successful():
            executable_id = n_sucessful_so_far + n_successful + 1
            insert_query = f"""
                INSERT INTO {EXECUTABLE_QUERIES_TABLE_NAME} (id, query_id, runnable_sql)
                VALUES ({executable_id}, {query_id}, '{escape_sql(result.query)}')
            """
            con.execute(insert_query)
            n_successful += 1
        else:
            global error_df
            error_df = pd.concat([error_df, pd.DataFrame([{
                'error_type': 'query_execution',
                'repo_id': repo_id,
                'repo_url': repo_url,
                'query_id': query_id,
                'table_name': None,
                'error_message': str(result.error),
                'sql': sql_prepared
            }])], ignore_index=True)
            n_failed += 1
            continue

    return n_successful, n_failed



def iterate_through_repos():

    con = duckdb.connect(DATABASE_PATH, read_only=False)

    repos = con.execute(f"""
        SELECT repos.id, repos.repo_url, COUNT(queries.id) AS query_count
        FROM repos
        JOIN queries ON repos.id = queries.repo_id
        WHERE queries.type IN ('SELECT')
        --  AND repo_id = 6044
        GROUP BY repos.id, repos.repo_url
        HAVING COUNT(queries.id) > 0
    """).fetchall()

    n_sucessful = 0
    n_failed = 0

    # create executable_queries table if it doesn't exist
    con.execute(f"""
        CREATE OR REPLACE TABLE {EXECUTABLE_QUERIES_TABLE_NAME} (
            id BIGINT {primary_key()},
            query_id BIGINT {foreign_key(QUERIES_TABLE_NAME, 'id')},
            runnable_sql VARCHAR
        )
    """)

    for repo_id, repo_url, cnt in tqdm(repos, desc="Processing repositories", unit="repo"):
        print(f"Processing repository {repo_id} - {repo_url}")

        sandbox_con = duckdb.connect()

        # add all the macros from EXTRA_FUNCTIONS
        for function in EXTRA_FUNCTIONS:
            sandbox_con.execute(function)

        tables = create_tables(repo_id, repo_url, con, sandbox_con)
        n_success_run, n_error_run = execute_queries(repo_id, repo_url, con, sandbox_con, n_sucessful, tables)

        n_sucessful += n_success_run
        n_failed += n_error_run

        sandbox_con.close()
        print(f"Executed {n_success_run}/{n_success_run+n_error_run} queries in repository {repo_id} - {repo_url}")
        print(f"Total successful queries: {n_sucessful}, Total failed queries: {n_failed}")
        print(f"Total successful table creations: {n_successful_table_creations}, Total failed table creations: {n_failed_table_creations}")

    # Save error DataFrame to CSV
    if not error_df.empty:
        # Create directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        csv_path = os.path.join('logs', 'sql_execution_errors.csv')
        error_df.to_csv(csv_path, index=False)
        print(f"Error log saved to {csv_path}")
    else:
        print("No errors occurred during execution")


if __name__ == "__main__":
    iterate_through_repos()
