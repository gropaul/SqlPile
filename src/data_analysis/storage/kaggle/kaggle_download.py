# Install dependencies as needed:
# pip install kagglehub[polars-datasets]
import os
import shutil
import threading
from time import sleep
from typing import List
import re

import kagglehub
from tqdm import tqdm

from src.data_analysis.storage.kaggle.kaggle_unify_schema import unify_kaggle_table_schema
import duckdb
from src.sql_analysis.utils.names import clean_name

from src.config import KAGGLE_DATA_DB_PATH, KAGGLE_DATASETS_DB_PATH, MAX_VALUES_TO_DOWNLOAD, \
    MAX_GB_TO_DOWNLOAD_PER_REPO, gb_to_bytes, MAX_GB_TO_DOWNLOAD


def table_name_from_file(file: str) -> str:
    file_name = file.split('/')[-1]
    return clean_name(file_name.split('.')[0])


def get_database_file_name_root(file: str) -> str:
    table_name = table_name_from_file(file)
    # remove all numbers or number-number combinations from the end of the string
    cleaned = re.sub(r'^[\d._-]+|[\d._-]+$', '', table_name)
    return cleaned


def log_error(datasets_con: duckdb.DuckDBPyConnection, handle: str, file: str, error_message: str):
    print(f"Error downloading {file} from {handle}: {error_message}")
    datasets_con.execute("""
                        INSERT INTO kaggle_dataset_download_errors (dataset_ref, file_name, error_message)
                        VALUES (?, ?, ?)
                    """, (handle, file, error_message))


def try_clear_cache():
    cache_path = os.path.expanduser("~/.cache/kagglehub/datasets")
    if os.path.exists(cache_path):
        try:
            shutil.rmtree(cache_path)  # deletes the entire folder and its contents
        except Exception as e:
            print(f"Error clearing kagglehub cache: {e}")

def import_file(file_path: str, con: duckdb.DuckDBPyConnection, schema_name: str, table_name: str):

    timeout_sec = 600
    def on_cancel():
        con.interrupt()
        raise Exception(f"Download timed out after {timeout_sec} seconds.")

    file_extension = file_path.split('.')[-1].lower()
    timer = threading.Timer(timeout_sec, on_cancel)
    timer.start()

    if file_extension == 'csv':
        con.execute(f"""CREATE TABLE "{schema_name}"."{table_name}" AS SELECT * FROM read_csv('{file_path}', strict_mode = False, ignore_errors = True) LIMIT {MAX_VALUES_TO_DOWNLOAD}""")
    elif file_extension == 'parquet':
        con.execute(f"""CREATE TABLE "{schema_name}"."{table_name}" AS SELECT * FROM read_parquet('{file_path}') LIMIT {MAX_VALUES_TO_DOWNLOAD}""")
    elif file_extension == 'sqlite':
        # For sqlite, we need to attach the database and then read from it
        con.execute(f"ATTACH OR REPLACE DATABASE '{file_path}' AS sqlite_db")
        tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_catalog='sqlite_db'").fetchall()
        for (table_name,) in tqdm(tables, desc="Importing sqlite tables", unit="table"):
            con.execute(f"""CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" AS SELECT * FROM sqlite_db."{table_name}" LIMIT {MAX_VALUES_TO_DOWNLOAD}""")
        # Detach the sqlite database after importing
        con.execute("DETACH DATABASE sqlite_db")
    else:
        timer.cancel()
        os.remove(file_path)
        raise Exception(f"Unsupported file extension: {file_extension}")

    timer.cancel()
    os.remove(file_path)

def download_dataset(
        handle: str, schema_name: str,
        files: List[str],
        data_con: duckdb.DuckDBPyConnection,
        datasets_con: duckdb.DuckDBPyConnection,
) -> int:

    data_con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
    try_clear_cache()

    bytes_downloaded_so_far = 0

    for file_dict in files:

        file, table_name, size = file_dict['file'], file_dict['table_name'], file_dict['size']
        print(f"\tDownloading file {file} into table {schema_name}.{table_name}")
        try:

            sleep(3)  # be nice to the kaggle servers
            path = kagglehub.datasets.dataset_download(handle, file)
            import_file(path, data_con, schema_name, table_name)
            unify_kaggle_table_schema(schema_name)

            bytes_downloaded_so_far += size
            if bytes_downloaded_so_far >=  gb_to_bytes(MAX_GB_TO_DOWNLOAD_PER_REPO):
                print(f"Reached max GB of {MAX_GB_TO_DOWNLOAD_PER_REPO} for dataset {handle}, stopping further downloads.")
                break
        except Exception as e:
            error_message = str(e)
            log_error(datasets_con, handle, file, error_message)

    return bytes_downloaded_so_far



def download_datasets(reset_errors: bool = False):
    datasets_con = duckdb.connect(KAGGLE_DATASETS_DB_PATH)

    datasets_con.create_function("clean_name", clean_name)
    datasets_con.create_function("table_name_from_file", table_name_from_file)
    datasets_con.create_function("get_database_file_name_root", get_database_file_name_root)

    # attach the kaggle_data database
    datasets_con.execute(f"ATTACH '{KAGGLE_DATA_DB_PATH}' AS kaggle_data")

    if reset_errors:
        datasets_con.execute("DROP TABLE IF EXISTS kaggle_dataset_download_errors")
    # create a table for all the errors
    datasets_con.execute("""
        CREATE TABLE IF NOT EXISTS kaggle_dataset_download_errors (
            dataset_ref TEXT,
            file_name TEXT,
            error_message TEXT
        )
    """)


    datasets = datasets_con.execute(
        f"""
        WITH existing_tables AS (
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_catalog = 'kaggle_data'
        ),
        filtered_datasets AS (
            SELECT dataset_ref, get_database_file_name_root(file_name) AS stem, MIN(file_name) as file_name
            FROM kaggle_dataset_files
            WHERE 
                lower(split(parse_filename(file_name, false), '.')[-1]) IN ('parquet', 'csv', 'sqlite') 
                AND total_bytes < {gb_to_bytes(MAX_GB_TO_DOWNLOAD_PER_REPO)}  -- less than max per repo
                AND total_bytes > 0  -- more than 0 bytes
            GROUP BY dataset_ref, stem
        ),
        data AS (
            SELECT ref, download_count, clean_name(ref) AS schema_name, file_name as file, total_bytes, table_name_from_file(file) AS table_name
            FROM kaggle_datasets
            JOIN filtered_datasets ON kaggle_datasets.ref = filtered_datasets.dataset_ref
        ),
        data_filtered_for_processed AS (
            SELECT data.*
            FROM data
            ANTI JOIN existing_tables 
                ON existing_tables.table_schema = data.schema_name
                AND existing_tables.table_name = data.table_name
            ANTI JOIN kaggle_dataset_download_errors
                ON kaggle_dataset_download_errors.dataset_ref = data.ref
                AND kaggle_dataset_download_errors.file_name = data.file
        )
        SELECT 
            ref, 
            schema_name,
            download_count,
            list({{
                file: file,
                table_name: table_name,
                size: total_bytes
            }}) AS files
        FROM data_filtered_for_processed
        GROUP BY ALL
        ORDER BY download_count DESC
        """
    ).fetchall()

    n_files = sum([len(d[3]) for d in datasets])

    print(f"Found {len(datasets)} datasets to download, with a total of {n_files} files.")

    datasets_con.execute(f"DETACH kaggle_data")
    data_con = duckdb.connect(KAGGLE_DATA_DB_PATH)

    total_bytes_downloaded = 0

    for ref, schema_name, view_count, files in datasets:
        print(f"Downloading {ref} with {len(files)} files with {view_count} views into schema {schema_name}")
        total_bytes_downloaded += download_dataset(ref, schema_name, files, data_con, datasets_con)

        if total_bytes_downloaded >= gb_to_bytes(MAX_GB_TO_DOWNLOAD):
            print(f"Reached overall max GB of {MAX_GB_TO_DOWNLOAD} for all datasets, stopping further downloads.")
            break



if __name__ == "__main__":
    download_datasets(reset_errors=True)
