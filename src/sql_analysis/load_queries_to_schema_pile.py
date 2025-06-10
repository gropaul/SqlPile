import os

import duckdb

from src.config import DATA_DIR, QUERIES_DIR, DATABASE_PATH
from src.sql_analysis.load_schema_pile_to_duckdb import QUERIES_TABLE_NAME, REPO_TABLE_NAME


def load_queries_to_schema_pile():
    # remove the old database if it exists
    db_path = os.path.join(DATABASE_PATH)
    queries_path = os.path.join(DATA_DIR, QUERIES_DIR + '/') + '*/*.json'
    con = duckdb.connect(db_path)

    # Ask the user if they want to (re)import the data, as the old data will be removed
    confirm = input("This will remove the old queries table and import the new data. Do you want to continue? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Aborting the import.")
        return
    print("Importing queries...")

    view_query = """
        CREATE OR REPLACE VIEW json_queries_tmp AS 
        SELECT
            repo_url, file_path, 
            CAST(unnest(queries).sql as VARCHAR) as sql, 
            CAST(unnest(queries).type as VARCHAR) as type,
            CAST(unnest(queries).line as Uint64) as line,
            CAST(unnest(queries).text_before as VARCHAR) as text_before,
            CAST(unnest(queries).text_after as VARCHAR) as text_after
        FROM '{}'
        """.format(queries_path)
    print(view_query)
    con.execute(view_query)

    # get the number of queries
    count = con.execute("SELECT COUNT(*) FROM json_queries_tmp").fetchone()[0]
    print(f"Found {count} queries to import.")

    # positional join with range(count) to add an id column
    con.execute(f"CREATE OR REPLACE VIEW json_queries AS SELECT * FROM json_queries_tmp POSITIONAL JOIN (SELECT range as id FROM range(0, {count}))")

    con.execute(f"DROP TABLE IF EXISTS {QUERIES_TABLE_NAME}")
    query = f"""
        CREATE TABLE {QUERIES_TABLE_NAME} AS (SELECT js.id as id, repo.id as repo_id, js.file_path as file_path, js.sql[2:-2] as sql, js.line as line FROM json_queries as js
        JOIN {REPO_TABLE_NAME} AS repo ON js.repo_url = repo.repo_url ORDER BY js.id) 
        """
    print(query)
    con.execute(query)

    # print first 10 queries
    rows = con.execute(f"SELECT * FROM {QUERIES_TABLE_NAME} LIMIT 10").fetchall()
    for row in rows:
        print(row)


if __name__ == "__main__":
    load_queries_to_schema_pile()
