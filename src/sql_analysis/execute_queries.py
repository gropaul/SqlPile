import json
import logging
from typing import List, Optional, Literal

import duckdb
from tqdm import tqdm

from src.config import DATABASE_PATH, get_con, MAX_VALUES_TO_SAVE_PER_COLUMN, REPO_TABLE_NAME, TABLES_TABLE_NAME, \
    TABLES_DATA_FILES_TABLE_NAME, COLUMNS_TABLE_NAME, COLUMN_VALUES_TABLE_NAME, TABLE_VALUES_COUNT_TABLE_NAME, \
    COLUMN_USAGES_TABLE_NAME, COLUMN_USAGES_HISTORY_TABLE_NAME, QUERIES_EXECUTABLE_TABLE_NAME, \
    QUERIES_ERROR_SELECT_TABLE_NAME, QUERIES_ERROR_CREATE_TABLE_NAME, QUERIES_ERROR_CREATE_VIEW_TABLE_NAME, \
    QUERIES_ERROR_INSERT_TABLE_NAME, QUERIES_TABLE_NAME
from src.sql_analysis.execution.create_tables import create_base_tables
from src.sql_analysis.execution.extra_functions import EXTRA_FUNCTIONS
from src.sql_analysis.execution.fix_group_by import fix_group_by
from src.sql_analysis.execution.mock_query import MockQueryResult, try_to_mock_and_execute_query
from src.sql_analysis.execution.models import Table, Column, ExecutionMode
from src.sql_analysis.execution.prepare_sql_for_execution import prepare_select_statically, escape_for_insert
from src.sql_analysis.execution.transform_insert import transform_insert_to_create, save_schema_from_insert_create
from src.sql_analysis.get_schemas_from_create_query import save_table_in_db
from src.sql_analysis.plan_analysis.analyse_plans import analyse_plans
from src.sql_analysis.tools.sql_to_schema import parse_create_table
from src.sql_analysis.tools.sql_types import base_type_to_duckdb_type, base_type_to_example_value


# Define the error table name

def quote(column_name: str) -> str:
    # if the column name has ` or ' in it, replace them with double quotes
    column_name = column_name.replace('`', '"').replace("'", '"')

    # if the column name is not already wrapped in quotes, wrap it in double quotes
    if not (column_name.startswith('"') and column_name.endswith('"')):
        return f'"{column_name}"'

    return column_name


n_failed_view_creations = 0
n_successful_view_creations = 0

n_failed_insertions = 0
n_successful_insertions = 0

EXCLUDED_REPOS = [16340]


class IDManager:
    def __init__(self, con: duckdb.DuckDBPyConnection):
        error_id_res = con.execute(f"SELECT MAX(id) FROM {QUERIES_ERROR_SELECT_TABLE_NAME}").fetchone()
        self.error_id = error_id_res[0] if error_id_res and error_id_res[0] is not None else 0

        success_id_res = con.execute(f"SELECT MAX(id) FROM {QUERIES_EXECUTABLE_TABLE_NAME}").fetchone()
        self.success_id = success_id_res[0] if success_id_res and success_id_res[0] is not None else 0

        self.n_success = 0
        self.n_error = 0


    def get_success_id(self) -> int:
        self.success_id += 1
        self.n_success += 1
        return self.success_id

    def get_error_id(self) -> int:
        self.error_id += 1
        self.n_error += 1
        return self.error_id




def make_create_statement(complete_quoted_table_name, columns):
    col_defs = []
    for column in columns:
        col_name = quote(column["column_name"])
        col_type = base_type_to_duckdb_type(column["column_base_type"])
        col_defs.append(f"{col_name} {col_type}")

    col_defs_str = ",\n".join(col_defs)

    create_statement = f"""
        CREATE TABLE IF NOT EXISTS {complete_quoted_table_name} (
            {col_defs_str}
        )"""
    return create_statement


def create_sandbox_tables(repo_id: int, con: duckdb.DuckDBPyConnection,
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
            create_statement = make_create_statement(complete_quoted_table_name, columns)
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
                                 sandbox_con: duckdb.DuckDBPyConnection) -> List[Table]:
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

    new_tables = []

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

            exception_message = str(e)
            is_table_does_not_exist = 'Catalog Error: Table with name' in exception_message
            if is_table_does_not_exist:
                create_statement = transform_insert_to_create(sql_prepared)
                if create_statement is not None:
                    try:
                        # Execute the create statement in the sandbox connection
                        sandbox_con.execute(create_statement)
                        logging.info(f"Created table from insert query: {create_statement}")
                        new_table = save_schema_from_insert_create(repo_id, create_statement, con, sandbox_con)
                        if new_table is not None:
                            new_tables.append(new_table)

                            # saving the newly created table to the database
                            table_schema = parse_create_table(sql)
                            save_table_in_db(con, repo_id, table_schema)

                    except Exception as e_create:
                        logging.error(f"Failed to create table from insert query: {e_create}")
                        continue

            global n_failed_insertions

            con.execute(f"""
                INSERT INTO {QUERIES_ERROR_INSERT_TABLE_NAME} (query_id, error_message)
                VALUES ({query_id}, '{escape_for_insert(str(e))}')
            """)
            n_failed_insertions += 1

    return new_tables


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
        if quote(table_name.lower()) not in existing_table_names:
            logging.error(f"Table {table_name} not found in repo {repo_id} for file {file_url} - Existing tables: {existing_table_names}")
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
            logging.info(f"Checking table {table.table_name} with ID {table.table_id} for population")
            # Check if the table is empty
            count = sandbox_con.execute(f"SELECT COUNT(*) FROM {quote(table.table_name)}").fetchone()[0]
            logging.info(f"Table {table.table_name} has {count} rows")
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


def execute_query(query_id: int, sql: str, sql_prepared: str, repo_id: int, repo_url: str,
                  con: duckdb.DuckDBPyConnection,
                  sandbox_con: duckdb.DuckDBPyConnection, tables: List[Table], id_manager: IDManager) -> MockQueryResult:
    result: MockQueryResult = try_to_mock_and_execute_query(sandbox_con, sql_prepared, tables)
    if result.was_successful():
        success_id = id_manager.get_success_id()
        insert_query = f"""
                        INSERT INTO {QUERIES_EXECUTABLE_TABLE_NAME} (id, query_id, repo_id, original_sql, executable_sql, logical_plan, logical_plan_optimized, logical_plan_optimized_detailed, physical_plan)
                        VALUES ({success_id}, {query_id}, {repo_id}, '{escape_for_insert(sql)}', '{escape_for_insert(result.executable_sql)}', 
                        '{escape_for_insert(json.dumps(result.logical_plan))}', 
                        '{escape_for_insert(json.dumps(result.logical_plan_optimized))}', 
                        '{escape_for_insert(json.dumps(result.logical_plan_optimized_detailed))}',
                        '{escape_for_insert(json.dumps(result.physical_plan))}')
                    """
        con.execute(insert_query)
    else:
        # try to fix group by errors
        fixed_sql = fix_group_by(sql_prepared, str(result.error), sandbox_con)
        if fixed_sql:
            sql = fixed_sql
            sql_prepared = fixed_sql
            return execute_query(query_id, sql, sql_prepared, repo_id, repo_url, con, sandbox_con, tables, id_manager)


        error_id = id_manager.get_error_id()
        con.execute(f"""
             INSERT INTO {QUERIES_ERROR_SELECT_TABLE_NAME} (
                 id, repo_id, repo_url, query_id, error_message, original_sql, executable_sql
             ) VALUES (
                 {error_id}, {repo_id}, '{escape_for_insert(repo_url)}', {query_id}, 
                 '{escape_for_insert(str(result.error))}', '{escape_for_insert(sql)}', '{escape_for_insert(result.executable_sql)}'
             )
         """)

    return result


def execute_queries(repo_id: int, repo_url: str, sandbox_con: duckdb.DuckDBPyConnection, con: duckdb.DuckDBPyConnection,
                    tables: List[Table], id_manager: IDManager, query_id: Optional[int] = None):
    logging.info(f"Executing queries for repo {repo_id} ({repo_url}). Loading queries...")
    queries_deduped = con.execute(f"""
        SELECT MIN(id), sql, MIN(prepare_select_statically(sql)) as sql_perpared
        FROM queries
        WHERE 
            repo_id = ? 
            AND type IN ('SELECT', 'WITH') 
            AND ({'queries.id = ' + str(query_id) if query_id else 'True'}) 
            AND queries.id NOT IN (SELECT query_id FROM executed_queries_ids)
        GROUP BY sql
    """, (repo_id,)).fetchall()
    # logging.warning('Number of queries is limited to 5000 per repo for now.')
    logging.info(f"Found {len(queries_deduped)} unique queries to execute for repo {repo_id} ({repo_url})")

    for query_id, sql, sql_prepared in tqdm(queries_deduped, desc="Executing queries", unit="query"):
        execute_query(query_id, sql, sql_prepared, repo_id, repo_url, con, sandbox_con, tables, id_manager)


def get_tables_from_create_statements(repo_id: int, con: duckdb.DuckDBPyConnection):

    create_queries = con.execute(f"""
    WITH distrinct_queries AS (
        SELECT sql, id, repo_id, type
        FROM {QUERIES_TABLE_NAME}
        WHERE 
            repo_id = {repo_id}
            AND type = 'CREATE' 
            AND not is_create_view_udf(sql)
    )
    SELECT sql, get_table_name_udf(sql) AS query_table_name, queries.id, queries.repo_id
    FROM distrinct_queries AS queries
    WHERE query_table_name IS NOT NULL
      AND repo_id = {repo_id}
      AND query_table_name IS NOT NULL 
      AND NOT EXISTS (
        FROM tables 
        WHERE 
            tables.repo_id = {repo_id}
            AND lower(tables.table_name_clean) = query_table_name
      ) 
    ORDER BY repo_id
    """).fetchall()

    if not create_queries:
        logging.info(f"No new CREATE TABLE queries found for repo {repo_id}")
        return

    for sql, table_name, query_id, repo_id in tqdm(create_queries, desc="Processing CREATE TABLE queries", unit="query"):
        try:
            table_schema = parse_create_table(sql)
            save_table_in_db(con, repo_id, table_schema)

        except Exception as e:
            con.execute(f"""
                INSERT INTO queries_parsing_error (repo_id, query_id, sql, error_message)
                VALUES (?, ?, ?, ?)
            """, (repo_id, query_id, sql, str(e)))

def save_string_column_values(repo_id: int, sandbox_con: duckdb.DuckDBPyConnection, con: duckdb.DuckDBPyConnection,
                              artificial_populated_ids: List[int]):

    print(f"The following tables were artificially populated: {artificial_populated_ids}")
    # get the columns that where recorded in the executable queries and for which we don't have values yet
    columns_to_record = con.execute(f"""
         SELECT {COLUMNS_TABLE_NAME}.id as column_id, column_name, table_name 
         FROM {COLUMNS_TABLE_NAME} 
         JOIN {TABLES_TABLE_NAME} on {COLUMNS_TABLE_NAME}.table_id = {TABLES_TABLE_NAME}.id
         WHERE 
            column_base_type = 'Text' 
            and {TABLES_TABLE_NAME}.repo_id = {repo_id}
            -- and  {COLUMNS_TABLE_NAME}.id IN (
            --     SELECT DISTINCT unnest(column_ids)
            --     FROM column_usages 
            --     JOIN queries ON queries.id = query_id 
            --     WHERE queries.repo_id = {repo_id}
            -- ) 
            and {TABLES_TABLE_NAME}.id NOT IN {artificial_populated_ids}
            and {COLUMNS_TABLE_NAME}.id NOT IN (SELECT DISTINCT column_id FROM {COLUMN_VALUES_TABLE_NAME})
    """).fetchall()
    for (column_id, column_name, table_name) in columns_to_record:

        try:
            values_arrow = sandbox_con.execute(f"""
                SELECT {column_id} as column_id, {quote(column_name)} as value
                FROM {quote(table_name)} 
                USING SAMPLE {MAX_VALUES_TO_SAVE_PER_COLUMN}
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


def save_table_counts(repo_id: int, con: duckdb.DuckDBPyConnection, sandbox_con: duckdb.DuckDBPyConnection):
    table_counts = con.execute(f"""
        SELECT id, table_name_clean 
        FROM {TABLES_TABLE_NAME} 
        WHERE 
            repo_id = {repo_id}
            AND id NOT IN (SELECT table_id FROM {TABLE_VALUES_COUNT_TABLE_NAME})
    """).fetchall()

    for (table_id, table_name) in table_counts:
        try:
            count = sandbox_con.execute(f"SELECT COUNT(*) FROM {quote(table_name)}").fetchone()[0]
            con.execute(f"""
                INSERT INTO {TABLE_VALUES_COUNT_TABLE_NAME} (table_id, count)
                VALUES ({table_id}, {count})
            """)
        except Exception as e:
            print(f"Failed to get count for table {table_name} with ID {table_id}: {e}")
            continue


def create_views(repo_id: int, repo_url: str, con: duckdb.DuckDBPyConnection,
                 sandbox_con: duckdb.DuckDBPyConnection):
    view_queries = con.execute(f"""
        WITH repo_queries AS MATERIALIZED (
            SELECT id, sql,
            FROM queries
            WHERE repo_id = ? AND type = 'CREATE'
        )
        SELECT id, sql, prepare_select_statically(sql) as sql_prepared
        FROM repo_queries
        WHERE is_create_view_udf(sql)
    """, (repo_id,)).fetchall()

    n_views = len(view_queries)
    n_success = 0

    for query_id, sql, sql_prepared in view_queries:
        try:
            # if there are multiple queries together, take the first one
            if ';' in sql_prepared:
                sql_prepared = sql_prepared.split(';')[0].strip()

            # Execute the prepared SQL statement
            sandbox_con.execute(sql_prepared)
            n_success += 1

            global n_successful_view_creations
            n_successful_view_creations += 1

        except Exception as e:
            global n_failed_view_creations
            con.execute(f"""
                INSERT INTO {QUERIES_ERROR_CREATE_VIEW_TABLE_NAME} (query_id, error_message)
                VALUES ({query_id}, '{escape_for_insert(str(e))}')
            """)
            n_failed_view_creations += 1
    if n_views > 0:
        logging.info(
            f"Processed {n_views} view creation queries in repo {repo_id} ({repo_url}), successfully created {n_success} views, failed to create {n_views - n_success} views.")




def execute_repo_queries(mode: ExecutionMode, repo_id: Optional[int] = None, query_id: Optional[int] = None):
    con = get_con()

    # check the number of tables for each 3rd party repo
    res = con.execute(f"""
        SELECT repos.id, repos.repo_url, COUNT(tables.id) AS table_count
        FROM repos
        LEFT JOIN tables ON repos.id = tables.repo_id
        WHERE '3rd-party' IN repos.repo_name
        GROUP BY repos.id, repos.repo_url
        ORDER BY repos.id DESC
    """).fetchall()

    for repo_id_, repo_url, table_count in res:
        if table_count == 0:
            print(f"Warning: Repo {repo_id_} ({repo_url}) has no tables.")

        print(f"Repo {repo_id_} ({repo_url}) has {table_count} tables.")

    create_base_tables(con, mode)

    con.close()
    con = get_con(DATABASE_PATH)

    repos = con.execute(f"""
        SELECT repos.id, repos.repo_url, COUNT(queries.id) AS query_count
        FROM repos
        JOIN queries ON repos.id = queries.repo_id
        WHERE 
            queries.type IN ('SELECT', 'WITH') 
            and ({'queries.id = ' + str(query_id) if query_id else 'True'})
            and ({'repos.id = ' + str(repo_id) if repo_id else 'True'})
            and repos.id NOT IN ({', '.join(map(str, EXCLUDED_REPOS))})
            and queries.id NOT IN (SELECT query_id FROM executed_queries_ids)
        GROUP BY repos.id, repos.repo_url
        HAVING COUNT(queries.id) > 0
        ORDER BY repos.id DESC -- from recently added to oldest
    """).fetchall()

    # create executable_queries table if it doesn't exist

    id_manager = IDManager(con)


    with tqdm(repos, desc="Processing repositories", unit="repo") as pbar:
        for repo_id, repo_url, cnt in pbar:
            logging.info(f"Processing repository {repo_id} ({repo_url}) with {cnt} queries")

            # initialize the sandbox connection
            sandbox_con = duckdb.connect()
            for function in EXTRA_FUNCTIONS:
                sandbox_con.execute(function)

            get_tables_from_create_statements(repo_id, con)

            tables = create_sandbox_tables(repo_id, con, sandbox_con)

            new_tables = populate_tables_with_inserts(repo_id, repo_url, con, sandbox_con)
            tables.extend(new_tables)

            create_views(repo_id, repo_url, con, sandbox_con)

            populate_tables_with_files(repo_id, con, sandbox_con, tables)
            artificial_populated_ids = populate_empty_tables(tables, sandbox_con)

            execute_queries(repo_id, repo_url, sandbox_con, con, tables,  id_manager, query_id)

            analyse_plans(con, repo_id)

            save_string_column_values(repo_id, sandbox_con, con, artificial_populated_ids)
            save_table_counts(repo_id, con, sandbox_con)

            # Close the sandbox connection, save progress, and checkpoint
            con.execute("CHECKPOINT;")
            sandbox_con.close()

            # Update counts
            error_count = id_manager.n_error
            success_count = id_manager.n_success
            total = success_count + error_count

            # Dynamically update tqdm description
            percent_success = (success_count / total * 100) if total > 0 else 0
            pbar.set_postfix({
                'Success Rate': f"{percent_success:.2f}%",
                'Success Count': success_count,
                'Usages': con.execute(f"SELECT COUNT(*) FROM {COLUMN_USAGES_TABLE_NAME}").fetchone()[0],
            })


    if n_failed_insertions > 0:
        print(
            f"Failed to insert {n_failed_insertions} rows into tables, successfully inserted {n_successful_insertions} rows.")
    # Check if any errors were recorded
    print(f"Successfully executed {success_count} queries across all repositories")
    if error_count > 0:
        print(f"{error_count} errors were recorded in the {QUERIES_ERROR_SELECT_TABLE_NAME} table")
    else:
        print("No errors occurred during execution")

    if n_successful_view_creations > 0 or n_failed_view_creations > 0:
        print(
            f"Successfully created {n_successful_view_creations} views, failed to create {n_failed_view_creations} views.")

    con.close()




if __name__ == "__main__":
    execute_repo_queries(mode='replace', repo_id=None, query_id=None)
