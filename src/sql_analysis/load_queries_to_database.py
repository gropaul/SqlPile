import os
from typing import List

import duckdb
import pandas as pd

from src.config import DATA_DIR, QUERIES_DIR, DATABASE_PATH, logger, COMBINED_QUERIES_PATH
from src.sql_analysis.load_schemapile_json_to_ddb import QUERIES_TABLE_NAME, REPO_TABLE_NAME


def get_all_parquet_files(root: str) -> List[str]:
    paths = []
    for dirpath, dirnames, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith(".parquet"):
                file_path = os.path.join(dirpath, filename)
                paths.append(file_path)

    return paths


# read these files and join them into one big file
def read_and_concat_parquet_files(file_paths: List[str]) -> pd.DataFrame:
    # Read all parquet files into a list of DataFrames
    dfs = []
    for file_path in file_paths:
        try:
            df = pd.read_parquet(file_path)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {file_path}: {str(e)}")

    if not dfs:
        raise ValueError("No valid parquet files were read")

    # Concatenate all DataFrames
    result_df = pd.concat(dfs, ignore_index=True)
    return result_df


def get_combined_parquet_file(root: str, result_file: str):
    try:
        paths = get_all_parquet_files(root)
        combined_df = read_and_concat_parquet_files(paths)
        # Save the combined DataFrame to a new parquet file
        combined_file_path = os.path.join(result_file)
        combined_df.to_parquet(combined_file_path, index=False)

        logger.info(f"Successfully combined {len(paths)} parquet files.")
        logger.info(f"Combined DataFrame shape: {combined_df.shape}")
    except Exception as e:
        logger.error(f"Error combining parquet files: {str(e)}")

def load_queries_to_database(ask: bool = True):

    # Ask the user if they want to (re)import the data, as the old data will be removed
    if ask:
        confirm = input(
            "This will remove the old queries table and import the new data. Do you want to continue? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Aborting the import.")
            return
        print("Importing queries...")

    # remove the old database if it exists
    db_path = os.path.join(DATABASE_PATH)
    queries_path = os.path.join(DATA_DIR, QUERIES_DIR)

    get_combined_parquet_file(queries_path, COMBINED_QUERIES_PATH)

    con = duckdb.connect(db_path)


    view_query = f"""
        CREATE OR REPLACE VIEW json_queries_tmp AS 
        SELECT
            repo_url, file_path, language as file_language,
            unnest(queries, recursive := True)
        FROM '{COMBINED_QUERIES_PATH}'
        """
    print(view_query)
    con.execute(view_query)

    # get the number of queries
    count = con.execute("SELECT COUNT(*) FROM json_queries_tmp").fetchone()[0]
    print(f"Found {count} queries to import.")

    # positional join with range(count) to add an id column
    con.execute(f"CREATE OR REPLACE VIEW json_queries AS SELECT * FROM json_queries_tmp POSITIONAL JOIN (SELECT range as id FROM range(0, {count}))")

    con.execute(f"DROP TABLE IF EXISTS {QUERIES_TABLE_NAME}")
    query = f"""
        CREATE TABLE {QUERIES_TABLE_NAME} AS (
            SELECT js.id as id, repo.id as repo_id, js.file_path as file_path, js.sql as sql, js.line as line, js.file_language as file_language,
            js.text_after as text_after, js.text_before as text_before, js.type as type
            FROM json_queries as js
        JOIN {REPO_TABLE_NAME} AS repo 
        ON js.repo_url = repo.repo_url 
        ORDER BY js.id
        )"""
    print(query)
    con.execute(query)

    # print first 10 queries
    rows = con.execute(f"SELECT * FROM {QUERIES_TABLE_NAME} LIMIT 10").fetchall()
    for row in rows:
        print(row)


if __name__ == "__main__":
    load_queries_to_database()
