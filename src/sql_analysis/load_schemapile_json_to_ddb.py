import os
import tqdm
import duckdb

from src.config import DATA_DIR, DATABASE_PATH
from src.sql_analysis.tools.semantic_type import get_column_semantic_type
from src.sql_analysis.tools.sql_types import unify_type

from src.sql_scraping.analyse_repo import get_repo_name_and_url
from src.sql_scraping.data_loading import read_schemapile_data

REPO_TABLE_NAME = 'repos'
TABLE_TABLE_NAME = 'tables'
COLUMNS_TABLE_NAME = 'columns'
COLUMN_USAGES_TABLE_NAME = 'column_usages'
QUERIES_TABLE_NAME = 'queries'
EXECUTABLE_QUERIES_TABLE_NAME = 'executable_queries'

repo_id_counter = 0
table_id_counter = 0
column_id_counter = 0

use_keys = False  # Use keys for primary keys and foreign keys

def primary_key() -> str:
    if not use_keys:
        return ''
    return 'PRIMARY KEY'


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

def process_repository(key: str, data: Dict[str, Dict], con: duckdb.DuckDBPyConnection) -> None:
    value = data[key]
    file_url = value['INFO']['URL']
    file_url = file_url.strip()  # Clean up the URL
    name, url = get_repo_name_and_url(file_url)
    # check if the repo already exists
    repo_id = con.execute(f"""
                SELECT id FROM {REPO_TABLE_NAME} WHERE repo_url = ?
            """, (url,)).fetchone()
    if repo_id is None:
        repo_id = get_id(REPO_TABLE_NAME)
        con.execute(f"""
                    INSERT INTO {REPO_TABLE_NAME} (id, repo_name, repo_url) VALUES (?, ?, ?)
                """, (repo_id, name, url))
    else:
        repo_id = repo_id[0]

    tables = value.get('TABLES', [])
    for table_key in tables:
        table_value = tables[table_key]
        table_name_clean = table_key.split('.')[-1]  # Get the table name from the key
        table_id = get_id(TABLE_TABLE_NAME)

        # check if the table already exists
        existing_table = con.execute(f"""
                    SELECT id FROM {TABLE_TABLE_NAME} WHERE repo_id = ? AND table_name = ?
                """, (repo_id, table_key)).fetchone()

        if existing_table is not None:
            continue

        con.execute(f"""
                    INSERT INTO {TABLE_TABLE_NAME} (id, repo_id, table_name, table_name_clean, file_url)
                    VALUES (?, ?, ?, ?, ?)
                """, (table_id, repo_id, table_key, table_name_clean, file_url))

        for column_key, column_value in table_value['COLUMNS'].items():
            column_id = get_id(COLUMNS_TABLE_NAME)
            column_type_original = column_value.get('TYPE', 'unknown')
            column_type, base_type = unify_type(column_type_original)
            is_unique = column_value.get('UNIQUE', False)
            is_nullable = column_value.get('NULLABLE', True)
            is_indexed = column_value.get('IS_INDEX', False)
            is_primary_key = column_value.get('IS_PRIMARY', False)

            semantic_type = get_column_semantic_type(column_key, base_type)

            con.execute(f"""
                        INSERT INTO {COLUMNS_TABLE_NAME} (
                            id, table_id, column_name, column_type, column_base_type,
                            column_type_original, semantic_type, is_unique, is_nullable,
                            is_indexed, is_primary_key
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                column_id, table_id, column_key, column_type, base_type,
                column_type_original, semantic_type, is_unique, is_nullable,
                is_indexed, is_primary_key
            ))


def load_schemapile_json_to_database(ask: bool = True) -> None:
    data = read_schemapile_data()

    # ask the user if the realy want to (re)inport the data, as the old data will be removed
    if ask:
        confirm = input(
            "This will remove the old database and import the new data. Do you want to continue? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Aborting the import.")
            return

    # remove the old database if it exists
    db_path = os.path.join(DATABASE_PATH)
    con = duckdb.connect(db_path)
    con.execute(f"""
        CREATE OR REPLACE TABLE {REPO_TABLE_NAME} (
            id BIGINT {primary_key()},
            repo_name VARCHAR,
            repo_url VARCHAR,
        )
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE  {TABLE_TABLE_NAME} (
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


    con = duckdb.connect(db_path)

    for key in tqdm.tqdm(data.keys(), desc="Loading data into DuckDB"):
        process_repository(key, data, con)


if __name__ == "__main__":
    load_schemapile_json_to_database()
