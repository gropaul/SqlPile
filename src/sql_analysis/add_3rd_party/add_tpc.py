import os

import duckdb
from typing import Literal, List

from src.config import DATABASE_PATH, DATA_DIR, TABLES_DATA_FILES_TABLE_NAME, TPC_DATA_DIR
from src.sql_analysis.add_3rd_party.utils import RepoTableData, RepoQuery, RepoData, add_3rd_party

Benchmark = Literal['tpc-h', 'tpc-ds']
SF = 0.5

def create_benchmark_data(benchmark: Benchmark):

    db_path = os.path.join(TPC_DATA_DIR, f'{benchmark}.duckdb')
    export_path = os.path.join(TPC_DATA_DIR, benchmark)
    queries_path = os.path.join(export_path, 'queries.csv')

    # remove the old database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)

    con = duckdb.connect(db_path)

    if benchmark == 'tpc-h':
        con.execute(f"CALL dbgen(sf={SF})")
    elif benchmark == 'tpc-ds':
        con.execute(f"CALL dsdgen(sf={SF})")
    else:
        raise ValueError(f"Unknown benchmark: {benchmark}")

    # export the data to parquet files
    con.execute(f"""
        EXPORT DATABASE '{export_path}' (FORMAT PARQUET, OVERWRITE TRUE)
    """)

    function_name = 'tpch_queries()' if benchmark == 'tpc-h' else 'tpcds_queries()'
    con.execute(f"COPY (SELECT * FROM {function_name}) TO '{queries_path}' (FORMAT CSV, HEADER TRUE)")

def add_tpc(benchmark: Benchmark, select_queries: List[str] = None, benchmark_name: str = None):

    # first we need to create the benchmark data
    create_benchmark_data(benchmark)

    con = duckdb.connect(DATABASE_PATH)
    tpch_dir = os.path.join(DATA_DIR, 'tpc', benchmark)
    queries_path = os.path.join(tpch_dir, 'queries.csv')
    schemas_path = os.path.join(tpch_dir, 'schema.sql')

    if select_queries is None:
        # read the queries from the csv file
        selects = con.execute(f"FROM '{queries_path}' ").fetchall()
        selects = [select[1] for select in selects if select[1].strip()]
    else :
        selects = select_queries

    schemas_file = open(schemas_path, 'r')
    creates = schemas_file.read()
    schemas_file.close()
    creates = creates.split(';')
    creates = [create.strip() for create in creates if create.strip()]

    all_queries = creates + selects
    query_types = ['CREATE'] * len(creates) + ['SELECT'] * len(selects)

    parquet_files = os.listdir(tpch_dir)
    parquet_files = [file for file in parquet_files if file.endswith('.parquet')]

    tabular_data_files: List[RepoTableData] = []
    for file in parquet_files:
        table_name = file.replace('.parquet', '')
        file_url = os.path.join(tpch_dir, file)
        tabular_data_files.append(RepoTableData(table_name=table_name, file_url=file_url))

    queries: List[RepoQuery] = []
    for query, query_type in zip(all_queries, query_types):
        queries.append(RepoQuery(query=query, query_type=query_type))

    if benchmark_name is None:
        benchmark_name = benchmark


    repo_data = RepoData(
        benchmark_name=benchmark_name,
        queries=queries,
        table_data=tabular_data_files
    )
    add_3rd_party(con, repo_data, replace_existing=True)



if __name__ == "__main__":
    add_tpc('tpc-h')
    add_tpc('tpc-ds')
