import os.path
import re
from codecs import ignore_errors
from typing import List

import duckdb
from tqdm import tqdm

from src.config import DATA_DIR

SQL_SHARE_DIR = os.path.join(DATA_DIR, 'sqlshare_data_release1')

con = duckdb.connect('sqlshare.duckdb')

def import_table(schema_name, table_name, path):
    # import the table using duckdb
    strict_mode = False
    null_padding = True
    ignore_errors = True
    con.sql(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')
    con.sql(f"""
        CREATE OR REPLACE TABLE "{schema_name}"."{table_name}" AS 
        SELECT * 
        FROM read_csv_auto("{path}", strict_mode={strict_mode}, null_padding={null_padding}, ignore_errors={ignore_errors});
    """)


def clean_and_split_sqlshare_script(content: str) -> List[str]:

    # all [xxxxxx.xxx] patters are replaced with [xxxxx]
    content = re.sub(r"\[(\w+)\.\w+(?:\.\w+)*\]", r"[\1]", content)

    statements = [sql.strip() for sql in content.split(DELIMITER) if sql.strip()]
    # replace [ and ] with "
    statements = [sql.replace('[', '"').replace(']', '"') for sql in statements]

    return statements

DELIMITER = '________________________________________'

def import_views():
    script_path = os.path.join(SQL_SHARE_DIR, 'view_script.txt')
    with open(script_path, 'r', encoding='utf-8') as file:
        content = file.read()

    statements = clean_and_split_sqlshare_script(content)
    n_errors = 0
    n_total = 0
    for statement in tqdm(statements, desc="Importing SQL Share views"):
        # print(statement)
        try:
            n_total += 1
            con.sql(statement)

        except Exception as e:
            n_errors += 1
            print(f"Error executing statement: {statement}\nError: {e}")
            continue

    print(f"Imported {n_total} views from SQL Share with {n_errors} errors.")


def run_queries():
    queries_path = os.path.join(SQL_SHARE_DIR, 'queries.txt')
    with open(queries_path, 'r', encoding='utf-8') as file:
        content = file.read()

    statements = clean_and_split_sqlshare_script(content)
    n_errors = 0
    n_total = 0
    for statement in tqdm(statements, desc="Running SQL Share queries"):
        # print(statement)
        try:
            n_total += 1
            con.sql(statement)

        except Exception as e:
            n_errors += 1
            print(f"Error executing statement: {statement}\nError: {e}")
            continue

    print(f"Executed {n_total} queries from SQL Share with {n_errors} errors.")

def import_datasets():
    SQL_SHARE_DATA_DIR = os.path.join(SQL_SHARE_DIR, 'data')
    n_total_tables = 0
    n_errors = 0
    # walk the elements in the directory
    for root, dirs, files in os.walk(SQL_SHARE_DATA_DIR):
        for dir in tqdm(dirs, desc="Importing SQL Share datasets"):
            schema_name = dir

            for tables in os.listdir(os.path.join(root, schema_name)):
                # print the table name
                table_name = tables.split('.')[0]
                table_path = os.path.join(root, schema_name, tables)


                try:
                    n_total_tables += 1
                    import_table(schema_name, table_name, table_path)
                except Exception as e:
                    print(f"Error importing {schema_name}.{table_name} from {table_path}: {e}")
                    n_errors += 1
    print(f"Imported {n_total_tables} tables from SQL Share datasets with {n_errors} errors.")


if __name__ == "__main__":
    import_datasets()
    import_views()
    run_queries()
    print("Schema Pile imported successfully.")