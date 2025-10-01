# Install dependencies as needed:
# pip install kagglehub[polars-datasets]
import os
import shutil
from time import sleep
from typing import List, Optional
import re

import kagglehub
from kagglehub import KaggleDatasetAdapter
import duckdb

from src.config import KAGGLE_DATA_DB_PATH, KAGGLE_DATASETS_DB_PATH


def clean_name(name: str) -> str:
    # only - delimiters are allowed in schema names
    name = (name
            .replace('_', '-')
            .replace('/', '-')
            .replace(' ', '-').lower())
    return name

def table_name_from_file(file: str) -> str:
    file_name = file.split('/')[-1]
    return clean_name(file_name.split('.')[0])

def get_database_file_name_root(file: str) -> str:
    table_name = table_name_from_file(file)
    # remove all numbers or number-number combinations from the end of the string
    cleaned = re.sub(r'^[\d._-]+|[\d._-]+$', '', table_name)
    return cleaned


def download_dataset(
        handle: str, schema_name: str,
        files: List[str],
        data_con: duckdb.DuckDBPyConnection,
        datasets_con: duckdb.DuckDBPyConnection,
        n_rows: Optional[int] = None):

    # create schema if not exists
    data_con.execute(f"""
        CREATE SCHEMA IF NOT EXISTS "{schema_name}"
    """)

    # clear the cache at ~/.cache/kagglehub/datasets
    # check if the dir exists
    cache_path = os.path.expanduser("~/.cache/kagglehub/datasets")
    if os.path.exists(cache_path):
        try:
            shutil.rmtree(cache_path)  # deletes the entire folder and its contents
        except Exception as e:
            print(f"Error deleting {cache_path}: {e}")

    else:
        print("Cache path does not exist, skipping cache clear.")

    pandas_kwargs = {
        "on_bad_lines": "skip",
    }
    if n_rows is not None:
        pandas_kwargs[0]["nrows"] = n_rows

    for file_dict in files:
        file = file_dict['file']
        table_name = file_dict['table_name']
        try:
            print(f"Loading {file} from {handle}")
            sleep(.25)  # be nice to the kaggle servers
            # an alternative would be kagglehub.dataset_download() to download the whole dataset
            # Load the latest version
            df = kagglehub.dataset_load(
                KaggleDatasetAdapter.PANDAS,
                handle=handle,
                path=file
            )

            print(f"Storing {file} as {handle}.{table_name} with {df.shape[0]} rows and {df.shape[1]} columns")
            data_con.execute(f"""
                        CREATE TABLE "{schema_name}"."{table_name}" AS SELECT * FROM df
                    """)
            print(f"Stored {file} as {handle}.{table_name}")
        except Exception as e:
            print(f"Error loading {file} from {handle}: {e}")
            datasets_con.execute("""
                INSERT INTO kaggle_dataset_download_errors (dataset_ref, file_name, error_message)
                VALUES (?, ?, ?)
            """, (handle, file, str(e)))


def main():
    datasets_con = duckdb.connect(KAGGLE_DATASETS_DB_PATH)

    datasets_con.create_function("clean_name", clean_name)
    datasets_con.create_function("table_name_from_file", table_name_from_file)
    datasets_con.create_function("get_database_file_name_root", get_database_file_name_root)

    # attach the kaggle_data database
    datasets_con.execute(f"ATTACH '{KAGGLE_DATA_DB_PATH}' AS kaggle_data")

    # create a table for all the errors
    datasets_con.execute("""
        CREATE TABLE IF NOT EXISTS kaggle_dataset_download_errors (
            dataset_ref TEXT,
            file_name TEXT,
            error_message TEXT
        )
    """)

    datasets = datasets_con.execute(
        """
        WITH existing_tables AS (
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_catalog = 'kaggle_data'
        ),
        filtered_datasets AS (
            SELECT dataset_id, get_database_file_name_root(file_name) AS stem, MIN(file_name) as file_name
            FROM kaggle_dataset_files
            WHERE 
                lower(split(parse_filename(file_name, false), '.')[-1]) IN ('csv', 'parquet')
                AND total_bytes < 10 * 10 ^ 9  -- less than 10 GB
                AND total_bytes > 0  -- more than 0 bytes
            GROUP BY dataset_id, stem
        ),
        data AS (
            SELECT ref, clean_name(ref) AS schema_name, file_name as file, table_name_from_file(file) AS table_name
            FROM kaggle_datasets
            JOIN filtered_datasets ON kaggle_datasets.id = filtered_datasets.dataset_id
            WHERE 
                'abhishekyana/nse-listed-1384' NOT IN ref
                AND 'allen-institute-for-ai' NOT IN ref -- too large for vldb wifi
                AND 'aymital/us-us' NOT IN ref -- too large for vldb wifi
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
            list({
                file: file,
                table_name: table_name
            }) AS files
        FROM data_filtered_for_processed
        GROUP BY ALL
        ORDER BY ref
        """
    ).fetchall()

    n_files = sum([len(d[2]) for d in datasets])

    print(f"Found {len(datasets)} datasets to download, with a total of {n_files} files.")
    data_con = duckdb.connect(KAGGLE_DATA_DB_PATH)

    for ref, schema_name, files in datasets:
        print(f"Downloading {ref} with {len(files)} files")
        download_dataset(ref, schema_name, files, data_con, datasets_con, n_rows=None)


if __name__ == "__main__":
    main()

