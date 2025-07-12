import os

from src.config import DATABASE_PATH, DATA_DIR
import duckdb


def add_tpch():
    con = duckdb.connect(DATABASE_PATH)
    queries_path = os.path.join(DATA_DIR, 'tpc', 'queries-tpc-h.csv')
    schemas_path = os.path.join(DATA_DIR, 'tpc', 'schema-tpc-h.sql')

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

    # insert the repo
    con.execute(f"""
        INSERT INTO repos (id, repo_name, repo_url)
        VALUES (?, ?, ?)
    """, (max_repo_id + 1, repo_name, repo_url))

    # insert the queries: id, file_id, repo_id, SQL, line, text_context, text_context_offset, type
    max_query_id = con.execute(f"""
        SELECT MAX(id) FROM queries
    """).fetchone()[0] or 0

    for i, (query, query_type) in enumerate(zip(all_queries, query_types)):
        con.execute(f"""
            INSERT INTO queries (id, file_id, repo_id, sql, line, text_context, text_context_offset, type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (max_query_id + i + 1, None, max_repo_id + 1, query.strip(), 0, '', 0, query_type))

    print(f"Added {len(all_queries)} queries to the database for repository {repo_name}, id {max_repo_id + 1}.")


if __name__ == "__main__":
    add_tpch()
