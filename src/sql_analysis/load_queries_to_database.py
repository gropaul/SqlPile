import os
from typing import List

import duckdb
import pandas as pd
from tqdm import tqdm

from src.config import DATABASE_PATH, logger, QUERIES_DIR_PARTITIONED, REPO_TABLE_NAME, FILES_TABLE_NAME, \
    FILES_META_DATA_TABLE_NAME, QUERIES_TABLE_NAME, QUERIES_DIR_RAW, QUERIES_LEGACY_DIR


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


def load_meta_data_files_to_database(con: duckdb.DuckDBPyConnection, repo_id: int):
    # Insert metadata for the specific repo
    insert_files_meta_data = f"""
        INSERT INTO {FILES_META_DATA_TABLE_NAME} (id, repo_id, file_path, file_type, content)
        SELECT 
            COALESCE((SELECT MAX(id) FROM {FILES_META_DATA_TABLE_NAME}), 0) + row_number() OVER () AS id,
            {repo_id} AS repo_id,
            file_path,
            file_type,
            content
        FROM (
            SELECT unnest(metadata_files, recursive := true)
            FROM parquet_queries_tmp_table
        ) AS unnested_files;
    """
    con.execute(insert_files_meta_data)


def create_missing_repo_for_url(con: duckdb.DuckDBPyConnection, repo_url: str) -> int:
    """
    Returns the repo_id
    """

    # check if the repo already exists
    repo_exists = con.execute(f"""
        SELECT id FROM {REPO_TABLE_NAME} WHERE repo_url = '{repo_url}'
    """).fetchone()
    if repo_exists is not None:
        logger.info(f"Repository {repo_url} already exists in the database.")
        return repo_exists[0]

    repo_id = con.execute(f"""
        SELECT COALESCE(MAX(id), 0) + 1 FROM {REPO_TABLE_NAME}
    """).fetchone()[0]

    repo_name = repo_url.split('/')[-1]  # Extract the repo name from the URL
    insert_repo_query = f"""
        INSERT INTO {REPO_TABLE_NAME} (id, repo_name, repo_url)
        VALUES ({repo_id}, '{repo_name}', '{repo_url}')
    """
    con.execute(insert_repo_query)

    logger.info(f"Inserted new repository {repo_name} with id {repo_id} into the database.")
    return repo_id



def load_queries_for_repo(repo_url: str, file_name: str, con: duckdb.DuckDBPyConnection):
    # Create a temporary table with queries for this repo only
    create_parquet_queries_with_dups = f"""
        CREATE OR REPLACE VIEW parquet_queries_tmp_table_with_dups AS (
            FROM read_parquet('{file_name}', union_by_name = true)
            WHERE repo_url = '{repo_url}'
        )
    """
    con.execute(create_parquet_queries_with_dups)

    logger.info(f"Created temporary table with queries from repo {repo_url}.")

    # *** CREATE A DEDUPLICATED TEMPORARY TABLE WITH REPO QUERIES ***
    create_parquet_queries = f"""
        CREATE OR REPLACE TEMP TABLE parquet_queries_tmp_table AS (
            SELECT repo_url, repo_name, arbitrary(file_results) as file_results, arbitrary(metadata_files) as metadata_files
            FROM parquet_queries_tmp_table_with_dups
            GROUP BY repo_url, repo_name
        )
    """
    con.execute(create_parquet_queries)
    logger.info(f"Created temporary table with deduplicated queries for repo {repo_url}.")

    repo_id = create_missing_repo_for_url(con, repo_url)
    logger.info(f"Inserted new repositories from queries into the repository table for repo {repo_url}.")

    # *** LOAD META DATA FILES ***
    load_meta_data_files_to_database(con, repo_id)
    logger.info(f"Loaded meta data files into the database for repo {repo_url}.")

    # *** CREATE FILES VIEW CONTAIN FILE INFORMATION AND QUERIES ***
    file_results_view_query = f"""
        CREATE OR REPLACE VIEW file_results_with_id AS
        WITH unnested as (SELECT 
            {repo_id} AS repo_id,
            p.repo_name,
            p.repo_url,
            unnest(file_results, max_depth := 2) AS file_result
         FROM parquet_queries_tmp_table AS p    
        )
        SELECT *, row_number() OVER () - 1 AS file_id
        FROM unnested  
    """
    con.execute(file_results_view_query)
    logger.info(f"Created view with file results and queries for repo {repo_url}.")

    # *** CREATE FILE TABLES ***
    query = f"""
        INSERT INTO {FILES_TABLE_NAME}
        SELECT file_id, repo_id, file_path as path, language, header, len(queries) as query_count
        FROM file_results_with_id
    """
    con.execute(query)
    logger.info(f"Inserted into files table with file information and queries for repo {repo_url}.")

    # *** CREATE QUERIES TABLE ***
    min_queries_id = con.execute(f"""
        SELECT COALESCE(MAX(id), 0) FROM {QUERIES_TABLE_NAME}
    """).fetchone()[0] or 0

    query = f"""
        INSERT INTO {QUERIES_TABLE_NAME}
        SELECT 
            {min_queries_id} + row_number() OVER () - 1 AS id,
            file_id,
            repo_id,
            sql,
            line,
            text_context,
            text_context_offset,
            type
        FROM (
            SELECT 
                file_id,
                repo_id,
                unnest(queries, recursive := true) AS q
            FROM file_results_with_id
        ) AS queries_unnested;
    """

    # language as file_language, file_path, header,
    logger.info(f"Inserting into queries table with query information for repo {repo_url}.")
    con.execute(query)



def load_queries_to_database(source_path: str, ask: bool = True):
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

    con = duckdb.connect(db_path)
    con.execute('SET preserve_insertion_order=false')
    con.execute('SET threads=8')

    # Create the tables before processing repos
    # Create FILES table
    con.execute(f"""
        CREATE OR REPLACE TABLE {FILES_TABLE_NAME} (
            file_id INTEGER,
            repo_id INTEGER,
            path VARCHAR,
            language VARCHAR,
            header VARCHAR,
            query_count INTEGER
        )
    """)
    logger.info("Created empty files table.")

    # Create QUERIES table
    con.execute(f"""
        CREATE OR REPLACE TABLE {QUERIES_TABLE_NAME} (
            id INTEGER,
            file_id INTEGER,
            repo_id INTEGER,
            sql VARCHAR,
            line INTEGER,
            text_context VARCHAR,
            text_context_offset INTEGER,
            type VARCHAR
        )
    """)
    logger.info("Created empty queries table.")

    # Create metadata files table
    create_table_if_not_exists = f"""
        CREATE OR REPLACE TABLE {FILES_META_DATA_TABLE_NAME} (
            id INTEGER,
            repo_id INTEGER,
            file_path VARCHAR,
            file_type VARCHAR,
            content VARCHAR
        )
    """
    con.execute(create_table_if_not_exists)
    logger.info("Created empty metadata files table.")

    # Get all unique repo URLs
    urls = con.execute(f"""
        SELECT repo_url, arbitrary(filename) FROM read_parquet('{source_path}', union_by_name = true)
        WHERE len(file_results) > 0
        GROUP BY repo_url
    """).fetchall()

    logger.info(f"Found {len(urls)} unique repositories to process.")

    # Process each repo individually to save memory
    for (url, file_name) in tqdm(urls, desc="Processing repositories", unit="repo"):
        logger.info(f"Processing repo: {url}")
        load_queries_for_repo(url, file_name, con)
        logger.info(f"Completed processing repo: {url}")

    # The remaining can be done for all at once
    # Get the count of queries imported
    count = con.execute(f"SELECT COUNT(*) FROM {QUERIES_TABLE_NAME}").fetchone()[0]
    logger.info(f"Imported {count} queries into the database.")

    # Print the number of queries where we found a repo_id and the number of queries without a repo_id using a group by
    repo_counts = con.execute(f"""
        SELECT repo_id is not null as has_repo, COUNT(*) as count
        FROM {QUERIES_TABLE_NAME}
        GROUP BY has_repo
    """).fetchall()

    # Print the types of the queries and the number of queries per type
    query_types = con.execute(f"""
        SELECT type, COUNT(*) as count 
        FROM {QUERIES_TABLE_NAME} 
        GROUP BY type 
        ORDER BY count DESC
    """).fetchall()

    print("\n*** Queries with and without repo_id ***")
    for has_repo, count in repo_counts:
        status = "with repo_id" if has_repo else "without repo_id"
        print(f"{status}: {count} queries")

    print("\n*** Query types and counts ***")
    for query_type, count in query_types:
        print(f"{query_type}: {count} queries")

    # load the meta data files to the database


if __name__ == "__main__":

    partioned_path = f'{QUERIES_DIR_PARTITIONED}/*/*.parquet'
    raw_path = f'{QUERIES_DIR_RAW}/*/*.parquet'
    combined = f'{QUERIES_DIR_RAW}/*/*.parquet'
    legacy = f'{QUERIES_LEGACY_DIR}/*/*.parquet'

    source = partioned_path

    load_queries_to_database(ask=False, source_path=legacy)
