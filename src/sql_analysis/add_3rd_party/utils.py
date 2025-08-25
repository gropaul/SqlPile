import os
from typing import List
import duckdb

from src.config import TABLES_DATA_FILES_TABLE_NAME, get_con


class RepoQuery:
    def __init__(self, query: str, query_type: str):
        self.query = query.strip()
        self.query_type = query_type.upper()  # Ensure query type is uppercase

class RepoTableData:
    def __init__(self, table_name: str, file_url: str):
        self.table_name = table_name
        self.file_url = file_url

class RepoData:
    def __init__(self, benchmark_name, queries: List[RepoQuery], table_data: List[RepoTableData]):
        self.repo_name = f'3rd-party-{benchmark_name}'
        self.repo_url = f'https://github.com/3rd-party/3rd-party-{benchmark_name}'
        self.queries: List[RepoQuery] = queries
        self.table_data: List[RepoTableData] = table_data

    @staticmethod
    def from_queries(benchmark_name, select_queries: List[str], create_queries: List[str], data_dir: str, data_file_endswith: str) -> 'RepoData':
        select_queries: List[RepoQuery] = [RepoQuery(query=query, query_type='SELECT') for query in select_queries]
        create_queries = [RepoQuery(query=query, query_type='CREATE') for query in create_queries
                          if query.strip().lower().startswith('create')]

        all_queries = select_queries + create_queries

        # get the benchmark's csv files
        data_files = [f for f in os.listdir(data_dir) if f.endswith(data_file_endswith)]

        tabular_data_files: List[RepoTableData] = []
        for file in data_files:
            table_name = file.replace(data_file_endswith, '')
            file_url = os.path.join(data_dir, file)
            tabular_data_files.append(RepoTableData(table_name=table_name, file_url=file_url))

        repo_data = RepoData(
            benchmark_name=benchmark_name,
            queries=all_queries,
            table_data=tabular_data_files
        )
        return repo_data


def add_3rd_party(con: duckdb.DuckDBPyConnection, repo_data: RepoData, replace_existing: bool = False):

    repo_name = repo_data.repo_name
    repo_url = repo_data.repo_url
    all_queries = repo_data.queries

    # check if the repo already exists
    repo_exists = con.execute(f"""
            SELECT id FROM repos WHERE repo_url = '{repo_url}'
        """).fetchone()
    if repo_exists is not None:
        if replace_existing:
            # we need to remove a) queries, b) columns c) tables d) table_data_files e) the repo itself
            repo_id = repo_exists[0]
            # a) remove queries and also from queries_error_select
            con.execute(f"""
                    DELETE FROM queries WHERE repo_id = {repo_id}
                """)
            con.execute(f"""
                    DELETE FROM queries_error_select WHERE repo_id = {repo_id}
                """)

            # b) remove columns, we need to join the tables as only the tables has the repo_id
            con.execute(f"""
                    DELETE FROM columns
                    WHERE table_id IN (SELECT id FROM tables WHERE repo_id = {repo_id})
                """)

            # c) remove tables
            con.execute(f"""
                    DELETE FROM tables WHERE repo_id = {repo_id}
                """)

            # d) remove table_data_files
            con.execute(f"""
                    DELETE FROM {TABLES_DATA_FILES_TABLE_NAME} WHERE repo_id = {repo_id}
                """)

            # e) remove the repo itself
            con.execute(f"""
                    DELETE FROM repos WHERE id = {repo_id}
                """)
        else:
            print("Skipping addition of repository due to existing data.")
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

    for i, (repo_query) in enumerate(repo_data.queries):
        repo_query: RepoQuery = repo_query
        con.execute(f"""
                INSERT INTO queries (id, file_id, repo_id, sql, line, text_context, text_context_offset, type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (max_query_id + i + 1, None, repo_id, repo_query.query, 0, '', 0, repo_query.query_type))

    print(f"Added {len(all_queries)} queries to the database for repository {repo_name}, id {repo_id}.")

    # Create table_data_files Table
    con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLES_DATA_FILES_TABLE_NAME} (
                id INTEGER,
                repo_id INTEGER,
                table_name TEXT,
                file_url TEXT
            )
        """)

    min_file_id = con.execute(f"""
            SELECT MAX(id) FROM table_data_files
        """).fetchone()[0] or 0


    for table_data in repo_data.table_data:
        con.execute(f"""
                INSERT INTO table_data_files (id, repo_id, table_name, file_url)
                VALUES (?, ?, ?, ?)
            """, (min_file_id + 1, repo_id, table_data.table_name, table_data.file_url))
        min_file_id += 1


