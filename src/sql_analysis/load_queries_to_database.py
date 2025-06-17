import os
from typing import List

import duckdb
import pandas as pd

from src.config import DATA_DIR, QUERIES_DIR, DATABASE_PATH
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

    con = duckdb.connect(db_path)

    view_query = f"""
        CREATE OR REPLACE VIEW parquet_queries_tmp AS (
            WITH 
                t1 AS (SELECT repo_name, repo_url, unnest(file_results) as file_results FROM '{QUERIES_DIR}/*/*.parquet'),
                t2 AS (SELECT repo_name, repo_url, file_results FROM t1 WHERE length(file_results.queries) > 1),
                t3 as (SELECT repo_name, repo_url, unnest(file_results) FROM t2)
            SELECT repo_name, repo_url, language as file_language, file_path,
                unnest(queries).type as type, unnest(queries).sql as sql , unnest(queries).line as line ,unnest(queries).text_context as text_context, unnest(queries).text_context_offset as text_context_offset
                FROM t3
        )
        """
    print(view_query)
    con.execute(view_query)

    # get the number of queries
    count = con.execute("SELECT COUNT(*) FROM parquet_queries_tmp").fetchone()[0]
    print(f"Found {count} queries to import.")

    # positional join with range(count) to add an id column
    con.execute(f"""
        CREATE OR REPLACE VIEW parquet_queries AS 
        SELECT * FROM parquet_queries_tmp POSITIONAL 
        JOIN (SELECT range as id FROM range(0, (SELECT COUNT(*) FROM parquet_queries_tmp)))
    """)

    con.execute(f"DROP TABLE IF EXISTS {QUERIES_TABLE_NAME}")
    query = f"""
        CREATE TABLE {QUERIES_TABLE_NAME} AS (
            SELECT pq.id as id, repo.id as repo_id, pq.file_path as file_path, pq.sql as sql, pq.line as line, pq.file_language as file_language,
            pq.text_context as text_context, pq.text_context_offset as text_context_offset, pq.type as type
            FROM parquet_queries as pq
        JOIN {REPO_TABLE_NAME} AS repo 
        ON pq.repo_url = repo.repo_url 
        ORDER BY pq.id
        )"""
    print(query)
    con.execute(query)

    # print first 10 queries
    rows = con.execute(f"SELECT * FROM {QUERIES_TABLE_NAME} LIMIT 10").fetchall()
    for row in rows:
        print(row)


if __name__ == "__main__":
    load_queries_to_database()
