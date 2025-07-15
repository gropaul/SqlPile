import os
from typing import List

import duckdb
import pandas as pd

from src.config import DATA_DIR, QUERIES_DIR, DATABASE_PATH, logger
from src.sql_analysis.load_schemapile_json_to_ddb import QUERIES_TABLE_NAME, REPO_TABLE_NAME, \
    FILES_META_DATA_TABLE_NAME, FILES_TABLE_NAME


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


def load_meta_data_files_to_database(con: duckdb.DuckDBPyConnection):
    # get all the meta data files saved in the DATA_DIR
    create_files_meta_data = f"""
        CREATE OR REPLACE TABLE {FILES_META_DATA_TABLE_NAME} AS (
            
            WITH files AS (
                SELECT repos.id AS repo_id, repo_url, *
                FROM (
                    SELECT repo_url, unnest(metadata_files, recursive := true) 
                    FROM '{QUERIES_DIR}/*/*.parquet'
                ) AS unnested_files
                JOIN {REPO_TABLE_NAME} AS repos USING (repo_url)
            ),
            numbered_ids AS (
                SELECT range AS file_id
                FROM range(0, (SELECT COUNT(*) FROM files))
            ),
            files_with_id AS (
                SELECT *, file_id
                FROM files
                POSITIONAL JOIN numbered_ids
            )
            SELECT file_id AS id, repo_id, file_path, file_type, content
            FROM files_with_id
        )
    """
    con.execute(create_files_meta_data)


def create_missing_repos_from_queries(con: duckdb.DuckDBPyConnection):
    insert_new_repos = f"""
        WITH new_repos AS (
            SELECT DISTINCT repo_url, split(repo_url, '/')[-1] AS repo_name
            FROM parquet_queries_with_id
            WHERE repo_url NOT IN (SELECT repo_url FROM {REPO_TABLE_NAME})
        ),
        existing_cnt AS (
            SELECT MAX(id) AS count FROM {REPO_TABLE_NAME}
        ),
        new_repos_cnt AS (
            SELECT COUNT(*) AS count FROM new_repos
        ),
        all_counts AS (
            SELECT existing_cnt.count AS existing_count, new_repos_cnt.count AS new_count
            FROM existing_cnt, new_repos_cnt
        ),
        numbered_ids AS (
            SELECT range AS id
            FROM all_counts, range(all_counts.existing_count + 1, all_counts.existing_count + all_counts.new_count + 1)
        ),
        new_repos_with_id AS (
            SELECT *, id
            FROM new_repos
            POSITIONAL JOIN numbered_ids
        )
        INSERT INTO {REPO_TABLE_NAME} (id, repo_name, repo_url)
        SELECT id, repo_name, repo_url FROM new_repos_with_id
    """
    con.execute(insert_new_repos).fetchall()


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
    con.execute('SET preserve_insertion_order=false')
    con.execute('SET threads=8')

    # create first a non-duplicated repo table becaus of out of memory issues with the parquet files
    create_parquet_queries_with_dups = f"""
            CREATE OR REPLACE TEMP TABLE parquet_queries_tmp_table_with_dups AS (FROM read_parquet('{QUERIES_DIR}/*/*.parquet', union_by_name = true))
        """
    con.execute(create_parquet_queries_with_dups)

    logger.info("Created temporary table with all queries from parquet files.")

    # *** CREATE A DEDUPLICATED TEMPORARY TABLE WITH ALL QUERIES ***
    create_parquet_queries = f"""
        CREATE OR REPLACE TEMP TABLE parquet_queries_tmp_table AS (
            SELECT repo_url, repo_name, MIN(file_results) as file_results , MIN(metadata_files) as metadata_files
            FROM parquet_queries_tmp_table_with_dups
            GROUP BY repo_url, repo_name
        )
    """
    con.execute(create_parquet_queries)

    logger.info("Created temporary table with deduplicated queries.")

    # Drop the temporary table with duplicates to free up memory
    con.execute("DROP TABLE IF EXISTS parquet_queries_tmp_table_with_dups")

    # *** CREATE PARQUET VIEW ***
    # positional join with range(count) to add an id column
    con.execute(f"""
          CREATE OR REPLACE TEMP TABLE parquet_queries_with_id AS 
          SELECT * FROM parquet_queries_tmp_table POSITIONAL 
          JOIN (SELECT range as id FROM range(0, (SELECT COUNT(*) FROM parquet_queries_tmp_table)))
      """)
    logger.info("Created temporary table with queries and added an id column.")

    create_missing_repos_from_queries(con)
    logger.info("Inserted new repositories from queries into the repository table.")

    # *** LOAD META DATA FILES ***
    load_meta_data_files_to_database(con)
    logger.info("Loaded meta data files into the database.")

    # *** CREATE FILES VIEW CONTAING FILE INFORMATINO AND QUERIES ***

    file_results_view_query = f"""
        CREATE OR REPLACE VIEW file_results_with_id AS (
            WITH 
                repo_results as (
                    SELECT id as repo_id, p.repo_name, p.repo_url, p.file_results as file_results, 
                    FROM parquet_queries_tmp_table as p
                    LEFT JOIN {REPO_TABLE_NAME} as r USING (repo_url)
                ),
                file_results AS (
                    SELECT repo_id, repo_name, repo_url, unnest(file_results) as file_results 
                    FROM repo_results
                ),
                file_results_filtered AS (
                    SELECT repo_id, repo_name, repo_url, file_results 
                    FROM file_results 
                    WHERE length(file_results.queries) > 1
                ),
                file_results_unnested as (
                    SELECT repo_id, repo_name, repo_url, unnest(file_results) 
                    FROM file_results_filtered
                ),
                file_ids as (SELECT range as file_id FROM range(0, (SELECT COUNT(*) FROM file_results_unnested))),
                file_results_with_file_id AS (
                    SELECT *
                    FROM file_results_unnested
                    POSITIONAL JOIN file_ids
                )
            SELECT * FROM file_results_with_file_id
        )
        """
    con.execute(file_results_view_query)
    logger.info("Created view with file results and queries.")

    # *** CREATE FILE TABLES ***

    query = f"""
        CREATE OR REPLACE TABLE {FILES_TABLE_NAME} AS (
            SELECT file_id, repo_id, file_path as path, language, header, len(queries) as query_count
            FROM file_results_with_id
        )
        """
    con.execute(query)
    logger.info("Created files table with file information and queries.")

    # *** CREATE QUERIES TABLE ***

    query = f"""
        CREATE OR REPLACE TABLE {QUERIES_TABLE_NAME} AS (
            WITH
                queries AS (
                    SELECT 
                        file_id, repo_id,
                        unnest(queries, recursive := true) AS query
                    FROM file_results_with_id
                ),
                query_ids AS (
                    SELECT range AS id FROM range(0, (SELECT COUNT(*) FROM queries))
                ),
                queries_with_id AS (
                    SELECT *, id
                    FROM queries
                    POSITIONAL JOIN query_ids
                )
            SELECT id, file_id, repo_id, sql, line, text_context, text_context_offset, type
            FROM queries_with_id
        )
    """

    # language as file_language, file_path, header,
    logger.info("Creating queries table with query information.")
    con.execute(query)

    # get teh count of queries imported
    count = con.execute(f"SELECT COUNT(*) FROM {QUERIES_TABLE_NAME}").fetchone()[0]
    logger.info(f"Imported {count} queries into the database.")

    # print the number of queries where we found a repo_id and the number of queries without a repo_id using a group by
    repo_counts = con.execute(f"""
        SELECT repo_id is not null as has_repo, COUNT(*) as count
        FROM {QUERIES_TABLE_NAME}
        GROUP BY has_repo
    """).fetchall()

    # print the types of the queries and the number of queries per type
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
    load_queries_to_database(ask=False)
