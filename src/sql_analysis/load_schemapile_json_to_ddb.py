import json
import logging
import os
import tqdm
import duckdb
import pandas as pd
from src.config import DATA_DIR, DATABASE_PATH, logger
from src.sql_analysis.tools.semantic_type import get_column_semantic_type
from src.sql_analysis.tools.sql_types import unify_type

from src.sql_scraping.analyse_repo import get_repo_name_and_url

REPO_TABLE_NAME = 'repos'
REPO_META_DATA_FILES_NAME = 'repos_meta_data'
FILES_META_DATA_NAME = 'files_meta_data'
TABLE_TABLE_NAME = 'tables'
COLUMNS_TABLE_NAME = 'columns'
COLUMN_USAGES_TABLE_NAME = 'column_usages'
QUERIES_TABLE_NAME = 'queries'
EXECUTABLE_QUERIES_TABLE_NAME = 'queries_executable'
ERROR_TABLE_NAME = 'queries_error'
repo_id_counter = 0
table_id_counter = 0
column_id_counter = 0

use_keys = True  # Use keys for primary keys and foreign keys

def primary_key() -> str:
    if not use_keys:
        return ''
    return 'PRIMARY KEY'

def unique_key() -> str:
    if not use_keys:
        return ''
    return 'UNIQUE'


def foreign_key(table_name: str, column_name: str) -> str:
    if not use_keys:
        return ''
    return f'REFERENCES {table_name}({column_name})'


def get_id(table_name: str) -> int:
    global repo_id_counter, table_id_counter, column_id_counter
    if table_name == REPO_TABLE_NAME:
        repo_id_counter += 1
        return repo_id_counter
    elif table_name == TABLE_TABLE_NAME:
        table_id_counter += 1
        return table_id_counter
    elif table_name == COLUMNS_TABLE_NAME:
        column_id_counter += 1
        return column_id_counter
    else:
        raise ValueError(f"Unknown table name: {table_name}")


from typing import List, Dict, Tuple

def extract_repositories_data(data: Dict[str, Dict]) -> pd.DataFrame:
    """Extract repository data from the JSON and return as a DataFrame."""
    repos = []
    for key in tqdm.tqdm(data.keys(), desc="Extracting repositories"):
        value = data[key]
        file_url = value['INFO']['URL'].strip()
        name, url = get_repo_name_and_url(file_url)
        repo_id = get_id(REPO_TABLE_NAME)
        repos.append({
            'id': repo_id,
            'repo_name': name,
            'repo_url': url
        })
    return pd.DataFrame(repos).drop_duplicates(subset=['repo_url'])

def extract_tables_data(data: Dict[str, Dict], repos_df: pd.DataFrame) -> pd.DataFrame:
    """Extract table data from the JSON and return as a DataFrame."""
    tables = []
    for key in tqdm.tqdm(data.keys(), desc="Extracting tables"):
        value = data[key]
        file_url = value['INFO']['URL'].strip()
        _, url = get_repo_name_and_url(file_url)

        # Find the repo_id from repos_df
        repo_row = repos_df[repos_df['repo_url'] == url]
        if repo_row.empty:
            continue
        repo_id = repo_row.iloc[0]['id']

        tables_data = value.get('TABLES', {})
        for table_key in tables_data:
            table_name_clean = table_key.split('.')[-1]
            table_id = get_id(TABLE_TABLE_NAME)
            tables.append({
                'id': table_id,
                'repo_id': repo_id,
                'table_name': table_key,
                'table_name_clean': table_name_clean,
                'file_url': file_url
            })
    return pd.DataFrame(tables).drop_duplicates(subset=['repo_id', 'table_name'])

def extract_columns_data(data: Dict[str, Dict], tables_df: pd.DataFrame) -> pd.DataFrame:
    """Extract column data from the JSON and return as a DataFrame."""
    columns = []
    for key in tqdm.tqdm(data.keys(), desc="Extracting columns"):
        value = data[key]
        tables_data = value.get('TABLES', {})

        for table_key in tables_data:
            table_value = tables_data[table_key]

            # Find the table_id from tables_df
            table_row = tables_df[tables_df['table_name'] == table_key]
            if table_row.empty:
                continue
            table_id = table_row.iloc[0]['id']

            for column_key, column_value in table_value.get('COLUMNS', {}).items():
                column_id = get_id(COLUMNS_TABLE_NAME)
                column_type_original = column_value.get('TYPE', 'unknown')
                column_type, base_type = unify_type(column_type_original)
                is_unique = column_value.get('UNIQUE', False)
                is_nullable = column_value.get('NULLABLE', True)
                is_indexed = column_value.get('IS_INDEX', False)
                is_primary_key = column_value.get('IS_PRIMARY', False)

                semantic_type = get_column_semantic_type(column_key, base_type)

                columns.append({
                    'id': column_id,
                    'table_id': table_id,
                    'column_name': column_key,
                    'column_type': column_type,
                    'column_base_type': base_type,
                    'column_type_original': column_type_original,
                    'semantic_type': semantic_type,
                    'is_unique': is_unique,
                    'is_nullable': is_nullable,
                    'is_indexed': is_indexed,
                    'is_primary_key': is_primary_key
                })
    # Create DataFrame with all columns
    column_names = [
        'id', 'table_id', 'column_name', 'column_type', 'column_base_type',
        'column_type_original', 'semantic_type', 'is_unique', 'is_nullable',
        'is_indexed', 'is_primary_key'
    ]
    if not columns:
        return pd.DataFrame(columns=column_names)
    return pd.DataFrame(columns, columns=column_names)


def read_schemapile_data():
    """Read the JSON data from schemapile-perm.json file."""
    # read the json schemapile-perm.json file
    path = os.path.join(DATA_DIR, 'schemapile-perm.json')
    with open(path, 'r') as file:
        data = json.load(file)
    return data

def load_schemapile_json_to_database(ask: bool = True) -> None:
    data = read_schemapile_data()

    # ask the user if they really want to (re)import the data, as the old data will be removed
    if ask:
        confirm = input(
            "This will remove the old database and import the new data. Do you want to continue? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Aborting the import.")
            return

    # First, extract all data into pandas DataFrames
    print("Extracting data into DataFrames...")
    repos_df = extract_repositories_data(data)
    print(f"Extracted {len(repos_df)} repositories")

    tables_df = extract_tables_data(data, repos_df)
    print(f"Extracted {len(tables_df)} tables")

    columns_df = extract_columns_data(data, tables_df)
    print(f"Extracted {len(columns_df)} columns")

    # Connect to the database
    db_path = os.path.join(DATABASE_PATH)
    con = duckdb.connect(db_path)

    # Create tables in DuckDB
    print("Creating tables in DuckDB...")
    con.execute(f"""
        CREATE OR REPLACE TABLE {REPO_TABLE_NAME} (
            id BIGINT {primary_key()},
            repo_name VARCHAR,
            repo_url VARCHAR {unique_key()}
        )
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE {TABLE_TABLE_NAME} (
            id BIGINT {primary_key()},
            repo_id BIGINT {foreign_key(REPO_TABLE_NAME, 'id')},
            table_name VARCHAR,
            table_name_clean VARCHAR,
            file_url VARCHAR
        )
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE {COLUMNS_TABLE_NAME} (
            id BIGINT {primary_key()},
            table_id BIGINT {foreign_key(TABLE_TABLE_NAME, 'id')},
            column_name VARCHAR,
            column_type VARCHAR,
            column_base_type VARCHAR,
            column_type_original VARCHAR,
            semantic_type VARCHAR,
            is_unique BOOLEAN,
            is_nullable BOOLEAN,
            is_indexed BOOLEAN,
            is_primary_key BOOLEAN
        )
    """)

    # Load DataFrames into DuckDB tables
    print("Loading DataFrames into DuckDB tables...")
    con.register('repos_df', repos_df)
    con.execute(f"INSERT INTO {REPO_TABLE_NAME} SELECT * FROM repos_df")

    con.register('tables_df', tables_df)
    con.execute(f"INSERT INTO {TABLE_TABLE_NAME} SELECT * FROM tables_df")

    con.register('columns_df', columns_df)
    con.execute(f"INSERT INTO {COLUMNS_TABLE_NAME} SELECT * FROM columns_df")

    print("Data loading complete!")


if __name__ == "__main__":
    load_schemapile_json_to_database()
