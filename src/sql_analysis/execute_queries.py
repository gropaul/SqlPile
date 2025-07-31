import json
import logging
from typing import List, Optional

import duckdb
from tqdm import tqdm

from src.config import DATABASE_PATH
from src.sql_analysis.execution.extra_functions import EXTRA_FUNCTIONS
from src.sql_analysis.execution.mock_query import MockQueryResult, try_to_mock_and_execute_query
from src.sql_analysis.execution.models import Table, Column
from src.sql_analysis.execution.prepare_sql_for_execution import prepare_select_statically, escape_for_insert, \
    prepare_sql_statically_macro
from src.sql_analysis.load_schemapile_json_to_ddb import primary_key, foreign_key, EXECUTABLE_QUERIES_TABLE_NAME, \
    REPO_TABLE_NAME, QUERIES_ERROR_SELECT_TABLE_NAME, QUERIES_ERROR_CREATE_TABLE_NAME, \
    COLUMN_VALUES_TABLE_NAME, COLUMNS_TABLE_NAME, TABLES_TABLE_NAME, COLUMN_USAGES_TABLE_NAME, \
    TABLES_DATA_FILES_TABLE_NAME, QUERIES_ERROR_INSERT_TABLE_NAME
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

EXCLUDED_REPOS = [16340]


def create_tables(repo_id: int, repo_url: str, con: duckdb.DuckDBPyConnection,
                  sandbox_con: duckdb.DuckDBPyConnection) -> List[Table]:
    tables_with_columns = con.execute("""
                                      SELECT table_id,
                                             FIRST(table_name),
                                             FIRST(table_name_clean),
                                             list_distinct(list({
                                                 'id': columns.id, 'column_index': columns.column_table_index,
                                                     'column_name': columns.column_name,
                                                 'column_base_type': columns.column_base_type
                                                 }))
                                      FROM tables
                                               JOIN columns ON tables.id = columns.table_id
                                      WHERE tables.repo_id = ?
                                      GROUP BY table_id
                                      """, (repo_id,)).fetchall()

    tables = []

    for table_id, table_name, table_name_clean, columns in tables_with_columns:

        # sort columns by their index
        columns.sort(key=lambda x: x['column_index'])

        # check if the table has a schema like `schema.table`
        if '.' in table_name:
            # if it does, create the schema if it doesn't exist
            schema_name, name_without_schema = table_name.split('.', 1)

            complete_quoted_table_name = f'{quote(schema_name)}.{quote(table_name_clean)}'
            try:
                sandbox_con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote(schema_name)}")
            except Exception as e:
                print(f"Failed to create schema {schema_name} for table {table_name}: {e}")
                continue

        else:
            complete_quoted_table_name = quote(table_name_clean)

        try:
            create_statement = f"""
                 CREATE TABLE IF NOT EXISTS {complete_quoted_table_name} ({
            ',\n'.join(
                f'{quote(column['column_name'])} {base_type_to_duckdb_type(column['column_base_type'])}'
                for column in columns)
            })"""
            create_statement = prepare_select_statically(create_statement)
            sandbox_con.execute(create_statement)

            table = Table(table_id, complete_quoted_table_name,
                          [Column(column['id'], column['column_name'], column['column_base_type']) for column in
                           columns])
            tables.append(table)
            global n_successful_table_creations
            n_successful_table_creations += 1
        except Exception as e:  # `trivia_user_cache`

            con.execute(f"""
                INSERT INTO {QUERIES_ERROR_CREATE_TABLE_NAME} (table_id, table_name, error_message)
                VALUES ({table_id}, '{escape_for_insert(table_name)}', '{escape_for_insert(str(e))}')
            """)
            continue
    return tables


def get_table_name_from_insert(sql: str) -> Optional[str]:
    """
    Extracts the table name from an INSERT INTO SQL query.
    Supports:
      - INSERT INTO table_name (column1, column2, ...)
      - INSERT INTO schema_name.table_name (column1, column2, ...)
    """
    import re

    # Remove extra whitespace and normalize casing for matching
    cleaned_sql = ' '.join(sql.strip().split())
    pattern = re.compile(
        r'INSERT\s+INTO\s+([`"\[\]\w\.]+)',  # Match table name (optionally qualified)
        re.IGNORECASE
    )

    match = pattern.match(cleaned_sql)
    if match:
        name = match.group(1).strip('`"[]')  # Remove optional backticks, quotes, etc.
        # if the table name is a qualified name (e.g., schema.table), return only the table name
        if '.' in name:
            name = name.split('.')[-1]
        return name.lower()
    else:
        return None


def populate_tables_with_inserts(repo_id: int, repo_url: str, con: duckdb.DuckDBPyConnection,
                                 sandbox_con: duckdb.DuckDBPyConnection):
    """
    Populate the tables with data from the repo.
    This function is called after the tables have been created.
    """
    # Get all insert queries for the repo
    insert_queries = con.execute(f"""
        SELECT 
            id, 
            sql, 
            prepare_select_statically(sql) as sql_prepared,
            sql LIKE '% select % from %' as is_insert_select
        FROM queries
        WHERE repo_id = ? AND type = 'INSERT'
        ORDER BY length(sql)
    """, (repo_id,)).fetchall()

    print(f"Found {len(insert_queries)} insert queries for repo {repo_id} ({repo_url})")

    for (query_id, sql, sql_prepared, is_insert_select) in insert_queries:
        try:

            if is_insert_select:
                # get the number of elements in the current table to now whether the insertion is not causing too
                # many values
                table_name = get_table_name_from_insert(sql_prepared)
                if table_name is None:
                    logging.error(f"Failed to extract table name from insert query: {sql}")
                    continue

                count = sandbox_con.execute(f"SELECT COUNT(*) FROM {quote(table_name)}").fetchone()[0]

                # for table names with fewer/equal than 3 characters, the limit is 10_000, otherwise 500_000
                if len(table_name) <= 3:
                    limit = 10_000
                else:
                    limit = 500_000

                if count > limit:
                    logging.warning(f"Skipping insert query for table {table_name} with {count} rows: {sql}")
                    continue

            global n_successful_insertions

            # if there are multiple queries together, take the first one
            if ';' in sql_prepared:
                sql_prepared = sql_prepared.split(';')[0].strip()

            # Execute the prepared SQL statement
            sandbox_con.execute(sql_prepared)
            n_successful_insertions += 1
        except Exception as e:
            global n_failed_insertions

            con.execute(f"""
                INSERT INTO {QUERIES_ERROR_INSERT_TABLE_NAME} (query_id, error_message)
                VALUES ({query_id}, '{escape_for_insert(str(e))}')
            """)
            n_failed_insertions += 1


def populate_tables_with_files(repo_id: int, con: duckdb.DuckDBPyConnection,
                               sandbox_con: duckdb.DuckDBPyConnection, tables: List[Table]):
    data_files = con.execute(f"""
        SELECT table_name, file_url
        FROM {TABLES_DATA_FILES_TABLE_NAME}
        WHERE repo_id = ?
    """, (repo_id,)).fetchall()

    existing_table_names = {table.table_name for table in tables}

    for (table_name, file_url) in data_files:
        # check if the table_name exists in the tables
        if quote(table_name) not in existing_table_names:
            logging.error(f"Table {table_name} not found in repo {repo_id} for file {file_url}")
            continue

        insert_query = f"""
            INSERT INTO {quote(table_name)}
            SELECT * FROM '{escape_for_insert(file_url)}'
        """
        try:
            sandbox_con.execute(insert_query)
        except Exception as e:
            logging.error(
                f"Failed to insert data from file {file_url} into table {table_name} in repo {repo_id}: {e}")


def populate_empty_tables(tables: List[Table], sandbox_con: duckdb.DuckDBPyConnection) -> List[int]:
    """
    Populates empty tables, returning a list of table IDs that were populated.
    """
    ids = []
    for table in tables:
        try:
            # Check if the table is empty
            count = sandbox_con.execute(f"SELECT COUNT(*) FROM {quote(table.table_name)}").fetchone()[0]
            if count == 0:
                # insert one valid and one null value into each table to confuse the optimizer
                # INSERT INTO table_name (column1, column2, column3, ...)
                # VALUES (value1, value2, value3, ...);

                columns = table.columns
                columns_list = ', '.join(quote(column.column_name) for column in columns)
                values_list = ', '.join(base_type_to_example_value(column.column_base_type) for column in columns)
                null_list = ', '.join('NULL' for _ in columns)

                insert_statement = f"""
                        INSERT INTO {table.table_name} ({columns_list})
                        VALUES ({values_list}), ({null_list});
                        """

                sandbox_con.execute(insert_statement)
                ids.append(table.table_id)
        except Exception as e:
            logging.error(f"Failed to populate table {table.table_name} with ID {table.table_id}: {e}")
            continue

    return ids


def execute_queries(repo_id: int, repo_url: str, sandbox_con: duckdb.DuckDBPyConnection, con: duckdb.DuckDBPyConnection,
                    tables: List[Table]):
    queries_deduped = con.execute(f"""
        SELECT MIN(id), sql, MIN(prepare_select_statically(sql)) as sql_perpared
        FROM queries
        WHERE 
            repo_id = ? AND 
            type IN ('SELECT', 'WITH') AND
            (id = 27602561 or True)  -- filter for a specific query or all queries
        GROUP BY sql
    """, (repo_id,)).fetchall()

    for query_id, sql, sql_prepared in queries_deduped:
        result: MockQueryResult = try_to_mock_and_execute_query(sandbox_con, sql_prepared, tables)

        if result.was_successful():
            global success_id_counter
            success_id_counter += 1
            insert_query = f"""
                INSERT INTO {EXECUTABLE_QUERIES_TABLE_NAME} (id, query_id, repo_id, original_sql, executable_sql, logical_plan, logical_plan_optimized, logical_plan_optimized_detailed, physical_plan)
                VALUES ({success_id_counter}, {query_id}, {repo_id}, '{escape_for_insert(sql)}', '{escape_for_insert(result.executable_sql)}', 
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
                INSERT INTO {QUERIES_ERROR_SELECT_TABLE_NAME} (
                    id, repo_id, repo_url, query_id, error_message, original_sql, executable_sql
                ) VALUES (
                    {error_id_counter}, {repo_id}, '{escape_for_insert(repo_url)}', {query_id}, 
                    '{escape_for_insert(str(result.error))}', '{escape_for_insert(sql)}', '{escape_for_insert(result.executable_sql)}'
                )
            """)
            continue


def save_used_column_values(repo_id: int, sandbox_con: duckdb.DuckDBPyConnection, con: duckdb.DuckDBPyConnection, artificial_populated_ids: List[int]):
    # get the columns that where recorded in the executable queries
    used_columns = con.execute(f"""
         SELECT {COLUMNS_TABLE_NAME}.id as column_id, column_name, table_name 
         FROM {COLUMNS_TABLE_NAME} 
         JOIN {TABLES_TABLE_NAME} on {COLUMNS_TABLE_NAME}.table_id = {TABLES_TABLE_NAME}.id
         WHERE 
            column_base_type = 'Text' and 
            {COLUMNS_TABLE_NAME}.id IN (
             SELECT DISTINCT unnest(column_ids)
             FROM column_usages 
             JOIN queries ON queries.id = query_id 
             WHERE queries.repo_id = {repo_id}
            ) and 
            {TABLES_TABLE_NAME}.id NOT IN {artificial_populated_ids}
    """).fetchall()
    for (column_id, column_name, table_name) in used_columns:

        try:
            values_arrow = sandbox_con.execute(f"""
                SELECT {column_id} as column_id, {column_name} as value
                FROM {quote(table_name)}
            """).arrow()

            if not values_arrow:
                continue

            try:
                con.execute(f"""
                    INSERT INTO {quote(COLUMN_VALUES_TABLE_NAME)} (column_id, value)
                    SELECT * FROM values_arrow
                """)
            except Exception as e:
                print(f"Failed to insert values' for column ID {column_id}: {e}")
                continue

        except Exception as e:
            print(f"Failed to execute query for column {column_name} in table {table_name}: {e}")
            continue


def execute_repo_queries(repo_id: Optional[int] = None):
    con = duckdb.connect(DATABASE_PATH, read_only=False)
    sandbox_con = duckdb.connect()

    # Add all the macros from EXTRA_FUNCTIONS
    for function in EXTRA_FUNCTIONS:
        sandbox_con.execute(function)

    repos = con.execute(f"""
        SELECT repos.id, repos.repo_url, COUNT(queries.id) AS query_count
        FROM repos
        JOIN queries ON repos.id = queries.repo_id
        WHERE 
            queries.type IN ('SELECT', 'WITH') 
            and ({'repos.id = ' + str(repo_id) if repo_id is not None else 'True'})
            and repos.id NOT IN ({', '.join(map(str, EXCLUDED_REPOS))})
        GROUP BY repos.id, repos.repo_url
        HAVING COUNT(queries.id) > 0
    """).fetchall()

    con.execute(prepare_sql_statically_macro)

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
        CREATE OR REPLACE TABLE {QUERIES_ERROR_SELECT_TABLE_NAME} (
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
        CREATE OR REPLACE TABLE {QUERIES_ERROR_CREATE_TABLE_NAME} (
            table_id BIGINT,
            table_name VARCHAR,
            error_message VARCHAR
        )
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE {QUERIES_ERROR_INSERT_TABLE_NAME} (
            query_id BIGINT,
            error_message VARCHAR,
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
               node_id VARCHAR,
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
            logging.info(f"Processing repository {repo_id} ({repo_url}) with {cnt} queries")

            # reset the sandbox connection
            table_names = sandbox_con.execute("SHOW ALL TABLES;").fetchall()
            for (database, schema, name, column_names, column_types, temporary) in table_names:
                try:
                    sandbox_con.execute(f'DROP TABLE "{database}"."{schema}"."{name}"')
                except Exception as e:
                    pass

            table_names = sandbox_con.execute("SHOW ALL TABLES;").fetchall()
            if len(table_names) != 0:
                logging.error("Not all tables have been deleted")

            tables = create_tables(repo_id, repo_url, con, sandbox_con)

            populate_tables_with_inserts(repo_id, repo_url, con, sandbox_con)
            populate_tables_with_files(repo_id, con, sandbox_con, tables)
            artificial_populated_ids = populate_empty_tables(tables, sandbox_con)

            execute_queries(repo_id, repo_url, sandbox_con, con, tables)

            analyse_plans(con, repo_id)

            save_used_column_values(repo_id, sandbox_con, con, artificial_populated_ids)

            con.execute("CHECKPOINT;")

            # Update counts
            error_count = con.execute(f"SELECT COUNT(*) FROM {QUERIES_ERROR_SELECT_TABLE_NAME}").fetchone()[0]
            success_count = con.execute(f"SELECT COUNT(*) FROM {EXECUTABLE_QUERIES_TABLE_NAME}").fetchone()[0]
            total = success_count + error_count

            # Dynamically update tqdm description
            percent_success = (success_count / total * 100) if total > 0 else 0
            pbar.set_postfix({
                'Success Rate': f"{percent_success:.2f}%",
                'Success Count': success_count,
                'Usages': con.execute(f"SELECT COUNT(*) FROM {COLUMN_USAGES_TABLE_NAME}").fetchone()[0],
            })

    # Print the failed table creation statistics
    print(
        f"Failed to create {n_failed_table_creations} tables, successfully created {n_successful_table_creations} tables.")
    if n_failed_insertions > 0:
        print(
            f"Failed to insert {n_failed_insertions} rows into tables, successfully inserted {n_successful_insertions} rows.")
    # Check if any errors were recorded
    print(f"Successfully executed {success_count} queries across all repositories")
    if error_count > 0:
        print(f"{error_count} errors were recorded in the {QUERIES_ERROR_SELECT_TABLE_NAME} table")
    else:
        print("No errors occurred during execution")

    con.close()


if __name__ == "__main__":
    execute_repo_queries()
