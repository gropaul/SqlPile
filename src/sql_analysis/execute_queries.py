import json
import os
from typing import List, Optional
from typing import Tuple

import duckdb
from tqdm import tqdm

from src.config import DATABASE_PATH
from src.sql_analysis.execution.extra_functions import EXTRA_FUNCTIONS
from src.sql_analysis.execution.mock_query import MockQueryResult, try_to_mock_and_execute_query
from src.sql_analysis.execution.models import Table, Column
from src.sql_analysis.execution.prepare_sql_for_execution import prepare_sql_statically, escape_for_insert
from src.sql_analysis.load_schemapile_json_to_ddb import primary_key, foreign_key, QUERIES_TABLE_NAME, \
    EXECUTABLE_QUERIES_TABLE_NAME, REPO_TABLE_NAME, ERRORS_QUERIES_TABLE_NAME, ERRORS_TABLES_TABLE_NAME, \
    COLUMN_VALUES_TABLE_NAME, COLUMNS_TABLE_NAME, TABLES_TABLE_NAME, COLUMN_USAGES_TABLE_NAME
from src.sql_analysis.plan_analysis.analyse_plans import analyse_plans
from src.sql_analysis.tools.sql_types import base_type_to_duckdb_type, base_type_to_example_value

# Define the error table name

def quote(column_name: str) -> str:
    # if the column name has ` or ' in it, replace them with double quotes
    column_name = column_name.replace('`', '"').replace("'", '"')

    # if the column name is not already wrapped in quotes, wrap it in double quotes
    if not (column_name.startswith('"') and column_name.endswith('"')):
        return f'"{column_name}"'

    return column_name

# Counter for error IDs
error_id_counter = 0
success_id_counter = 0

n_failed_table_creations = 0
n_successful_table_creations = 0

n_failed_insertions = 0
n_successful_insertions = 0

n_column_usage_insertions = 0

def create_tables(repo_id: int, repo_url: str, con: duckdb.DuckDBPyConnection, sandbox_con: duckdb.DuckDBPyConnection) -> List[Table]:

    tables_with_columns = con.execute("""
        SELECT table_id, ANY_VALUE(table_name), list_distinct(list({
            'id': columns.id,
            'column_index': columns.column_table_index,
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

        # sort columns by their index
        columns.sort(key=lambda x: x['column_index'])

        # check if the table has a schema like `schema.table`
        if '.' in table_name:
            # if it does, create the schema if it doesn't exist
            schema_name, name_without_schema = table_name.split('.', 1)

            complete_quoted_table_name = f'{quote(schema_name)}.{quote(name_without_schema)}'
            try:
                sandbox_con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote(schema_name)}")
            except Exception as e:
                print(f"Failed to create schema {schema_name} for table {table_name}: {e}")
                continue

        else:
            complete_quoted_table_name = quote(table_name)

        try:
            create_statement = f"""
                 CREATE TABLE IF NOT EXISTS {complete_quoted_table_name} ({
            ',\n'.join(
                f'{quote(column['column_name'])} {base_type_to_duckdb_type(column['column_base_type'])}'
                for column in columns)
            })"""
            create_statement = prepare_sql_statically(create_statement)
            sandbox_con.execute(create_statement)

            # insert one valid and one null value into each table to confuse the optimizer
            # INSERT INTO table_name (column1, column2, column3, ...)
            # VALUES (value1, value2, value3, ...);

            columns_list = ', '.join(quote(column['column_name']) for column in columns)
            values_list = ', '.join(base_type_to_example_value(column['column_base_type']) for column in columns)
            null_list = ', '.join('NULL' for _ in columns)

            insert_statement = f"""
            INSERT INTO {complete_quoted_table_name} ({columns_list})
            VALUES ({values_list}), ({null_list});
            """

            sandbox_con.execute(insert_statement)

            table = Table(table_id, complete_quoted_table_name,
                          [Column(column['id'], column['column_name'], column['column_base_type']) for column in
                           columns])
            tables.append(table)
            global n_successful_table_creations
            n_successful_table_creations += 1
        except Exception as e: #`trivia_user_cache`

            global n_failed_table_creations
            n_failed_table_creations += 1
            continue
    return tables


def populate_tables(repo_id: int, repo_url: str, con: duckdb.DuckDBPyConnection, sandbox_con: duckdb.DuckDBPyConnection):
    """
    Populate the tables with data from the repo.
    This function is called after the tables have been created.
    """
    # Get all insert queries for the repo
    insert_queries = con.execute(f"""
        SELECT sql
        FROM queries
        WHERE repo_id = ? AND type = 'INSERT'
        ORDER BY length(sql) DESC OFFSET 10
    """, (repo_id,)).fetchall()

    for (sql,) in insert_queries:
        try:
            global n_successful_insertions

            # Execute the insert query in the sandbox connection
            sql_prepared = prepare_sql_statically(sql)
            # if there are multiple queries together, take the first one
            if ';' in sql_prepared:
                sql_prepared = sql_prepared.split(';')[0].strip()

            # Execute the prepared SQL statement
            sandbox_con.execute(sql_prepared)
            n_successful_insertions += 1
        except Exception as e:
            global n_failed_insertions
            # print(f"Failed to execute insert query: {sql_prepared} with error: {e}")
            n_failed_insertions += 1




def execute_queries(repo_id: int, repo_url: str, sandbox_con: duckdb.DuckDBPyConnection, con: duckdb.DuckDBPyConnection, tables: List[Table]):

    queries_deduped = con.execute(f"""
        SELECT MIN(id), sql, 
        FROM queries
        WHERE repo_id = ? AND type IN ('SELECT', 'WITH')
        GROUP BY sql
    """, (repo_id,)).fetchall()

    for query_id, sql in queries_deduped:
        sql_prepared = prepare_sql_statically(sql)
        result: MockQueryResult = try_to_mock_and_execute_query(sandbox_con, sql_prepared, tables)

        if result.was_successful():
            global success_id_counter
            success_id_counter += 1
            insert_query = f"""
                INSERT INTO {EXECUTABLE_QUERIES_TABLE_NAME} (id, query_id, repo_id, original_sql, executable_sql, logical_plan, logical_plan_optimized, logical_plan_optimized_detailed, physical_plan)
                VALUES ({success_id_counter}, {query_id}, {repo_id}, '{escape_for_insert(result.original_query)}', '{escape_for_insert(result.executable_sql)}', 
                '{escape_for_insert(json.dumps(result.logical_plan))}', 
                '{escape_for_insert(json.dumps(result.logical_plan_optimized))}', 
                '{escape_for_insert(json.dumps(result.logical_plan_optimized_detailed))}',
                '{escape_for_insert(json.dumps(result.physical_plan))}')
            """
            con.execute(insert_query)
        else:
            global error_id_counter
            error_id_counter += 1
            con.execute(f"""
                INSERT INTO {ERRORS_QUERIES_TABLE_NAME} (
                    id, repo_id, repo_url, query_id, error_message, original_sql, executable_sql
                ) VALUES (
                    {error_id_counter}, {repo_id}, '{escape_for_insert(repo_url)}', {query_id}, 
                    '{escape_for_insert(str(result.error))}', '{escape_for_insert(result.original_query)}', '{escape_for_insert(result.executable_sql)}'
                )
            """)
            continue


def save_used_column_values(repo_id: int, sandbox_con: duckdb.DuckDBPyConnection, con: duckdb.DuckDBPyConnection):

    # get the columns that where recorded in the executable queries
    used_columns = con.execute(f"""
         SELECT {COLUMNS_TABLE_NAME}.id as column_id, column_name, table_name 
         FROM {COLUMNS_TABLE_NAME} 
         JOIN {TABLES_TABLE_NAME} on {COLUMNS_TABLE_NAME}.table_id = {TABLES_TABLE_NAME}.id
         WHERE column_base_type = 'Text' and {COLUMNS_TABLE_NAME}.id IN (
             SELECT DISTINCT unnest(column_ids)
             FROM column_usages 
             JOIN queries ON queries.id = query_id 
             WHERE queries.repo_id = {repo_id}
         )
    """).fetchall()
    for (column_id, column_name, table_name) in used_columns:

        try:
            values = sandbox_con.execute(f"""
                SELECT {column_id} as column_id, {column_name} as value
                FROM {quote(table_name)}
            """).fetchall()

            if not values:
                continue

            for (column_id, value) in values:
                try:
                    con.execute(f"""
                        INSERT INTO {quote(COLUMN_VALUES_TABLE_NAME)} (column_id, value)
                        VALUES ({column_id}, '{escape_for_insert(value)}')
                    """)
                except Exception as e:
                    print(f"Failed to insert value '{value}' for column ID {column_id}: {e}")
                    continue

        except Exception as e:
            print(f"Failed to execute query for column {column_name} in table {table_name}: {e}")
            continue



    


def iterate_through_repos():

    con = duckdb.connect(DATABASE_PATH, read_only=False)

    repos = con.execute(f"""
        SELECT repos.id, repos.repo_url, COUNT(queries.id) AS query_count
        FROM repos
        JOIN queries ON repos.id = queries.repo_id
        WHERE queries.type IN ('SELECT', 'WITH') and (repos.id = 24430 or true)
        GROUP BY repos.id, repos.repo_url
        HAVING COUNT(queries.id) > 0
    """).fetchall()

    # create executable_queries table if it doesn't exist
    con.execute(f"""
        CREATE OR REPLACE TABLE {EXECUTABLE_QUERIES_TABLE_NAME} (
            id BIGINT {primary_key()},
            query_id BIGINT,
            repo_id BIGINT,
            original_sql VARCHAR,
            executable_sql VARCHAR,
            logical_plan JSON,
            logical_plan_optimized JSON,
            logical_plan_optimized_detailed JSON,
            physical_plan JSON
        )
    """)

    # create error table if it doesn't exist
    con.execute(f"""
        CREATE OR REPLACE TABLE {ERRORS_QUERIES_TABLE_NAME} (
            id BIGINT {primary_key()},
            repo_id BIGINT {foreign_key(REPO_TABLE_NAME, 'id')},
            repo_url VARCHAR,
            query_id BIGINT,
            error_message VARCHAR,
            original_sql VARCHAR,
            executable_sql VARCHAR,
        )
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE {ERRORS_TABLES_TABLE_NAME} (
            id BIGINT {primary_key()},
            repo_id BIGINT {foreign_key(REPO_TABLE_NAME, 'id')},
            table_name VARCHAR,
            error_message VARCHAR
        )
    """)
    
    # Create a table to store the column values with strings
    con.execute(f"""
        CREATE OR REPLACE TABLE {COLUMN_VALUES_TABLE_NAME} (
            column_id BIGINT,
            value VARCHAR,
        )
    """)

    create_usages_table_query = f"""
           CREATE OR REPLACE TABLE {COLUMN_USAGES_TABLE_NAME} (
               id INTEGER,
               query_id INTEGER,
               column_ids INTEGER[],
               expression VARCHAR,
               expression_result_type VARCHAR,
               usage_type VARCHAR)
       """
    con.execute(create_usages_table_query)

    error_count = 0
    success_count = 0

    with tqdm(repos, desc="Processing repositories", unit="repo") as pbar:
        for repo_id, repo_url, cnt in pbar:

            sandbox_con = duckdb.connect(':memory:', read_only=False)

            # Add all the macros from EXTRA_FUNCTIONS
            for function in EXTRA_FUNCTIONS:
                sandbox_con.execute(function)

            tables = create_tables(repo_id, repo_url, con, sandbox_con)
            populate_tables(repo_id, repo_url, con, sandbox_con)
            execute_queries(repo_id, repo_url, sandbox_con, con, tables)

            analyse_plans(con, repo_id)
            save_used_column_values(repo_id, sandbox_con, con)
            
            sandbox_con.close()

            # Update counts
            error_count = con.execute(f"SELECT COUNT(*) FROM {ERRORS_QUERIES_TABLE_NAME}").fetchone()[0]
            success_count = con.execute(f"SELECT COUNT(*) FROM {EXECUTABLE_QUERIES_TABLE_NAME}").fetchone()[0]
            total = success_count + error_count

            # Dynamically update tqdm description
            percent_success = (success_count / total * 100) if total > 0 else 0
            pbar.set_postfix({
                'Success Rate': f"{percent_success:.2f}%",
                'Success Count': success_count,
            })


    # Print the failed table creation statistics
    print(f"Failed to create {n_failed_table_creations} tables, successfully created {n_successful_table_creations} tables.")
    if n_failed_insertions > 0:
        print(
            f"Failed to insert {n_failed_insertions} rows into tables, successfully inserted {n_successful_insertions} rows.")
    # Check if any errors were recorded
    print(f"Successfully executed {success_count} queries across all repositories")
    if error_count > 0:
        print(f"{error_count} errors were recorded in the {ERRORS_QUERIES_TABLE_NAME} table")
    else:
        print("No errors occurred during execution")

    con.close()



if __name__ == "__main__":
    iterate_through_repos()
