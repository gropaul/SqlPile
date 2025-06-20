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
    con.execute('SET preserve_insertion_order=false')
    con.execute('SET threads=8')

    queries_view = f"""
        CREATE OR REPLACE VIEW parquet_queries_tmp AS (
            WITH 
                t1 AS (SELECT repo_url, unnest(file_results) as file_results FROM '{QUERIES_DIR}/*/*.parquet'),
                t2 AS (SELECT repo_url, file_results FROM t1 WHERE length(file_results.queries) > 1),
                t3 as (SELECT repo_url, unnest(file_results) FROM t2)
            SELECT repo_url, 
                unnest(queries).type as type, unnest(queries).sql as sql , unnest(queries).line as line ,unnest(queries).text_context as text_context, unnest(queries).text_context_offset as text_context_offset
                FROM t3
        )
        """

    # language as file_language, file_path, header,
    print(queries_view)
    con.execute(queries_view)

    # get the number of queries
    count = con.execute("SELECT COUNT(*) FROM parquet_queries_tmp").fetchone()[0]
    print(f"Found {count} queries to import.")

    # positional join with range(count) to add an id column
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE parquet_queries AS 
        SELECT * FROM parquet_queries_tmp POSITIONAL 
        JOIN (SELECT range as id FROM range(0, (SELECT COUNT(*) FROM parquet_queries_tmp)))
    """)

    insert_new_repos = f"""
        WITH new_repos AS (
            SELECT DISTINCT repo_url, split(repo_url, '/')[-1] AS repo_name
            FROM parquet_queries
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
    print(insert_new_repos)
    new_repo_urls = con.execute(insert_new_repos).fetchall()

    query = f"""
        CREATE OR REPLACE TABLE {QUERIES_TABLE_NAME} AS (
            SELECT pq.id as id, repo.id as repo_id, pq.sql as sql, pq.line as line,
            pq.text_context as text_context, pq.text_context_offset as text_context_offset, pq.type as type
            FROM parquet_queries as pq
        LEFT JOIN {REPO_TABLE_NAME} AS repo using (repo_url)
        ORDER BY pq.id
        )"""
    print(query)
    con.execute(query)

    # get teh count of queries imported
    count = con.execute(f"SELECT COUNT(*) FROM {QUERIES_TABLE_NAME}").fetchone()[0]
    print(f"Imported {count} queries into the database.")

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


if __name__ == "__main__":
    load_queries_to_database(ask=False)
