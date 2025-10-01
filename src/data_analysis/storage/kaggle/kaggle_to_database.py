from typing import List, Optional
import duckdb

from src.config import KAGGLE_DATA_DB_PATH, get_con, COLUMNS_TABLE_NAME, TABLE_VALUES_COUNT_TABLE_NAME, \
    COLUMN_VALUES_TABLE_NAME, MAX_VALUES_TO_SAVE_PER_COLUMN
from src.data_analysis.storage.kaggle.kaggle_download import clean_name
from src.sql_analysis.add_3rd_party.utils import get_benchmark_repo_name, get_benchmark_repo_url
from src.sql_analysis.utils.delete_data import delete_repo
from src.sql_analysis.tools.sql_types import unify_type




def add_schema_as_repo(con: duckdb.DuckDBPyConnection, schema_name: str) -> Optional[int]:

    # only - delimiters are allowed in schema names
    schema_name = clean_name(schema_name)
    schema_name = f'kaggle-{schema_name}'

    repo_name = get_benchmark_repo_name(schema_name)

    # check if the repo already exists
    existing_repo = con.execute(f"""
        SELECT id FROM repos 
        WHERE repo_name = ?
    """, (repo_name,)).fetchone()

    if existing_repo is not None:
        existing_id = existing_repo[0]
        delete_repo(con, existing_id)

    max_repo_id = con.execute("SELECT MAX(id) FROM repos").fetchone()[0]
    repo_id = max_repo_id + 1 if max_repo_id is not None else 0

    repo_url = get_benchmark_repo_url(schema_name)
    con.execute("""
        INSERT INTO repos (id, repo_name, repo_url)
        VALUES (?, ?, ?)
    """, (repo_id, repo_name, repo_url))

    print(f"Inserted new repository {repo_name} with id {repo_id}.")
    return repo_id


def add_table_to_db(con: duckdb.DuckDBPyConnection, repo_id: int, schema_name: str, table_name: str, columns: List[dict]):

    # check if the table already exists
    table_name_clean = clean_name(table_name)

    existing_table = con.execute("""
        SELECT id FROM tables
        WHERE repo_id = ? AND table_name_clean = ?
    """, (repo_id, table_name_clean)).fetchone()

    if existing_table is not None:
        print(f"Table {table_name} already exists in repo {repo_id}. Skipping.")
        return

    # columns: Tables: id │ repo_id │ table_name │ table_name_clean | file_url
    table_id = con.execute("SELECT MAX(id) FROM tables").fetchone()[0]
    table_id = table_id + 1 if table_id is not None else 0

    con.execute("""
        INSERT INTO tables (id, repo_id, table_name, table_name_clean, file_url)
        VALUES (?, ?, ?, ?, ?)
    """, (table_id, repo_id, table_name, table_name_clean, None))

    row_count = con.execute(f"SELECT COUNT(*) FROM \"kaggle_data\".\"{schema_name}\".\"{table_name}\"").fetchone()[0]

    con.execute(f"""
        INSERT INTO {TABLE_VALUES_COUNT_TABLE_NAME} (table_id, count)
        VALUES (?, ?)
    """, (table_id, row_count))

    print(f"Inserted table {table_name} with id {table_id} and {row_count} rows.")

    for col in columns:
        column_id = con.execute("SELECT MAX(id) FROM columns").fetchone()[0]
        column_id = column_id + 1 if column_id is not None else 0

        column_name = col['column_name']
        column_table_index = col['ordinal_position']
        column_type_original = col['data_type']

        column_type, base_type = unify_type(column_type_original)

        con.execute(f"""
                             INSERT INTO {COLUMNS_TABLE_NAME} (
                                 id, table_id, column_name, column_table_index, column_type, column_base_type,
                                 column_type_original, semantic_type, is_unique, is_nullable,
                                 is_indexed, is_primary_key
                             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                         """, (
            column_id, table_id, column_name, column_table_index, column_type, base_type,
            column_type_original, '', False,
            True, False, False
        ))

        # if it is a base_type text, add n values to the string_data table
        if base_type == "Text":
            insert_query = f"""
                INSERT INTO {COLUMN_VALUES_TABLE_NAME}
                SELECT {column_id}, "{column_name}" AS value
                FROM "kaggle_data"."{schema_name}"."{table_name}"
                USING SAMPLE {MAX_VALUES_TO_SAVE_PER_COLUMN}
            """
            try:

                con.execute(insert_query)
            except Exception as e:
                print(f"Error inserting values for column {column_name} in table {table_name}: {str(e)}")
                print(f"Query: {insert_query}")
                print(f"Column ID: {column_id}, Table ID: {table_id}, Repo ID: {repo_id}")


def add_kaggle_schema(con: duckdb.DuckDBPyConnection, schema_name: str):

    # get the columns with their types and their table names
    # table_name, column_name, data_type
    print(f"Processing schema: {schema_name}")

    repo_id = add_schema_as_repo(con, schema_name)

    query = f"""
         SELECT table_name, list({{
             column_name: column_name,
             data_type: data_type,
             ordinal_position: ordinal_position
         }}) AS table_columns
         FROM information_schema.columns
         WHERE table_schema = '{schema_name}'
         GROUP BY table_name
         ORDER BY table_name
     """

    tables = con.execute(query).fetchall()

    for table_name, columns in tables:
        add_table_to_db(con, repo_id, schema_name, table_name, columns)



def save_kaggle_in_database():
    """
    This adds kaggle repositories to the database, as well as
    a) tables
    b) columns
    c) table_sizes
    d) string_data
    Each dataset is its own repository
    """

    con = get_con()
    con.execute(f"ATTACH DATABASE '{KAGGLE_DATA_DB_PATH}' AS kaggle_data")

    # get all the tables in the kaggle_data schema
    tables = con.execute("""
        SELECT DISTINCT table_schema
        FROM information_schema.tables 
        WHERE table_catalog = 'kaggle_data'
        GROUP BY table_schema
        ORDER BY table_schema
    """).fetchall()

    print(f"Found {len(tables)} kaggle schemas to process.")

    for schema_name, in tables:
        add_kaggle_schema(con, schema_name)



def delte_kaggle_repos():
    con = get_con()
    # get all the repos that start with kaggle-
    repos = con.execute("""
        SELECT id, repo_name FROM repos
        WHERE repo_name LIKE '3rd-party-kaggle-%'
    """).fetchall()

    print(f"Found {len(repos)} kaggle repos to delete.")

    for repo_id, repo_name in repos:
        print(f"Deleting repo {repo_name} with id {repo_id}")
        delete_repo(con, repo_id)


if __name__ == "__main__":
    delte_kaggle_repos()
    save_kaggle_in_database()
