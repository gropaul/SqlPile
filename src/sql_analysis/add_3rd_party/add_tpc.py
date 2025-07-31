import os

import duckdb

from src.config import DATABASE_PATH, DATA_DIR
from src.sql_analysis.load_schemapile_json_to_ddb import TABLES_DATA_FILES_TABLE_NAME


def add_tpch():

    con = duckdb.connect(DATABASE_PATH)
    tpch_dir = os.path.join(DATA_DIR, 'tpc', 'tpc-h')
    queries_path = os.path.join(tpch_dir, 'queries.csv')
    schemas_path = os.path.join(tpch_dir, 'schema.sql')

    selects = con.execute(f"FROM '{queries_path}' ").fetchall()
    schemas_file = open(schemas_path, 'r')
    creates = schemas_file.read()
    schemas_file.close()
    creates = creates.split(';')
    creates = [create.strip() for create in creates if create.strip()]
    selects = [select[1] for select in selects if select[1].strip()]

    all_queries = creates + selects
    query_types = ['CREATE'] * len(creates) + ['SELECT'] * len(selects)

    repo_name = '3rd-party-tpc-h'
    repo_url = 'https://github.com/3rd-party/3rd-party-tpc-h'

    # check if the repo already exists
    repo_exists = con.execute(f"""
        SELECT id FROM repos WHERE repo_url = '{repo_url}'
    """).fetchone()
    if repo_exists is not None:
        print(f"Repository {repo_name} already exists in the database. Id: {repo_exists[0]}.")
        return

    # get the max id for the repo
    max_repo_id = con.execute(f"""
        SELECT MAX(id) FROM repos
    """).fetchone()[0]

    repo_id = max_repo_id + 1 if max_repo_id is not None else 1

    # insert the repo
    con.execute(f"""
        INSERT INTO repos (id, repo_name, repo_url)
        VALUES (?, ?, ?)
    """, (repo_id, repo_name, repo_url))

    # insert the queries: id, file_id, repo_id, SQL, line, text_context, text_context_offset, type
    max_query_id = con.execute(f"""
        SELECT MAX(id) FROM queries
    """).fetchone()[0] or 0

    for i, (query, query_type) in enumerate(zip(all_queries, query_types)):
        con.execute(f"""
            INSERT INTO queries (id, file_id, repo_id, sql, line, text_context, text_context_offset, type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (max_query_id + i + 1, None, repo_id, query.strip(), 0, '', 0, query_type))

    print(f"Added {len(all_queries)} queries to the database for repository {repo_name}, id {repo_id}.")

    # Create table_data_files Table
    con.execute(f"""
        CREATE OR REPLACE TABLE {TABLES_DATA_FILES_TABLE_NAME} (
            id INTEGER,
            repo_id INTEGER,
            table_name TEXT,
            file_url TEXT
        )
    """)

    parquet_files = os.listdir(tpch_dir)
    parquet_files = [file for file in parquet_files if file.endswith('.parquet')]
    min_file_id = con.execute(f"""
        SELECT MAX(id) FROM table_data_files
    """).fetchone()[0] or 0
    for file in parquet_files:
        table_name = file.replace('.parquet', '')
        file_url = os.path.join(tpch_dir, file)
        con.execute(f"""
            INSERT INTO table_data_files (id, repo_id, table_name, file_url)
            VALUES (?, ?, ?, ?)
        """, (min_file_id + 1, repo_id, table_name, file_url))
        min_file_id += 1





if __name__ == "__main__":
    add_tpch()
