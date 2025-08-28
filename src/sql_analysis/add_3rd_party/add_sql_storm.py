from src.config import SQL_STORM_DATA_DIR, SQL_STORM_REPO_DIR, get_con
import requests
import os
import tarfile
import re
import duckdb

from src.sql_analysis.add_3rd_party.add_tpc import Benchmark, add_tpc
from src.sql_analysis.add_3rd_party.utils import RepoQuery, RepoTableData, RepoData, add_3rd_party


class StackOverflowDownloadResult:
    def __init__(self, schema_path: str, dir_path: str):
        self.schema_path = schema_path
        self.data_dir = dir_path


def download_stackoverflow_data() -> StackOverflowDownloadResult:
    output_dir = os.path.join(SQL_STORM_DATA_DIR, 'stackoverflow')
    # download the schema from https://db.in.tum.de/~schmidt/data/stackoverflow_schema.sql
    schema_url = 'https://db.in.tum.de/~schmidt/data/stackoverflow_schema.sql'
    schema_path = os.path.join(output_dir, 'schema_stackoverflow.sql')

    response = requests.get(schema_url)
    if response.status_code == 200:
        with open(schema_path, 'w') as f:
            f.write(response.text)
        print(f"Schema downloaded to {schema_path}")
    else:
        print(f"Failed to download schema: {response.status_code}")

    # download the data from https://db.in.tum.de/~schmidt/data/stackoverflow_dba.tar.gz

    data_url = 'https://db.in.tum.de/~schmidt/data/stackoverflow_dba.tar.gz'
    data_tar_path = os.path.join(output_dir, 'data_stackoverflow.tar.gz')
    response = requests.get(data_url)
    if response.status_code == 200:
        with open(data_tar_path, 'wb') as f:
            f.write(response.content)
        print(f"Data downloaded to {data_tar_path}")
    else:
        print(f"Failed to download data: {response.status_code}")

    tarfile_name = os.path.basename(data_tar_path)
    tarfile_dir_name = tarfile_name.split('.')[0]  # remove the .tar.gz extension
    # extract the tar.gz file
    with tarfile.open(data_tar_path, 'r:gz') as tar:
        tar.extractall(path=output_dir)
        data_tar_name = tar.getnames()[0]  # get the name of the extracted directory
        output_dir = os.path.join(output_dir, data_tar_name)
        print(f"Data extracted to {output_dir}")

    # remove the tar.gz file
    os.remove(data_tar_path)

    return StackOverflowDownloadResult(schema_path=schema_path, dir_path=output_dir)


def remove_multiline_comments(code: str) -> str:
    pattern = re.compile(r"/\*.*?\*/", re.DOTALL)
    return re.sub(pattern, "", code)


def add_sql_storm_stack_overflow():
    result = download_stackoverflow_data()

    schema_content = open(result.schema_path, 'r').read()
    schema_content = remove_multiline_comments(schema_content)
    schema_queries = schema_content.split(';')

    stackoverflow_queries_dir = os.path.join(SQL_STORM_REPO_DIR, 'v1.0', 'stackoverflow', 'queries')
    sql_files = [f for f in os.listdir(stackoverflow_queries_dir) if f.endswith('.sql')]
    benchmark_queries = [
        open(os.path.join(stackoverflow_queries_dir, sql_file), 'r').read() for sql_file in sql_files
    ]


    benchmark_name = 'sql-storm-stackoverflow'
    repo_data = RepoData.from_queries(benchmark_name, benchmark_queries, schema_queries, result.data_dir, '.csv')
    con = get_con()
    add_3rd_party(con, repo_data, replace_existing=True)


def add_sql_storm_tpc(tpc_benchmark: Benchmark):
    if tpc_benchmark == 'tpc-h':
        dir_name = 'tpch'
    elif tpc_benchmark == 'tpc-ds':
        dir_name = 'tpcds'
    else:
        raise ValueError(f"Unknown TPC benchmark: {tpc_benchmark}")
    stackoverflow_queries_dir = os.path.join(SQL_STORM_REPO_DIR, 'v1.0', dir_name, 'queries')

    sql_files = [f for f in os.listdir(stackoverflow_queries_dir) if f.endswith('.sql')]
    benchmark_queries = [
        open(os.path.join(stackoverflow_queries_dir, sql_file), 'r').read() for sql_file in sql_files
    ]

    add_tpc(benchmark=tpc_benchmark, benchmark_name=f'sql-storm-{tpc_benchmark}', select_queries=benchmark_queries)


def add_sql_storm_job():
    imdb_url_schema = 'https://raw.githubusercontent.com/duckdb/duckdb/37c5f11a0f83e0561b925a5dd26f1eda60013151/benchmark/imdb_plan_cost/init/schema.sql'
    imdb_url_load = 'https://raw.githubusercontent.com/duckdb/duckdb/1a29fb46c37dbd39d16b3d91e919336849040cec/benchmark/imdb_plan_cost/init/load.sql'

    imdb_schema = requests.get(imdb_url_schema).text
    imdb_load = requests.get(imdb_url_load).text

    imdb_db_path = os.path.join(SQL_STORM_DATA_DIR, 'imdb', 'imdb.duckdb')
    os.makedirs(os.path.dirname(imdb_db_path), exist_ok=True)
    con = duckdb.connect(imdb_db_path)

    if not os.path.exists(imdb_db_path):
        con.execute(imdb_schema)
        con.execute(imdb_load)

    export_path = os.path.join(SQL_STORM_DATA_DIR, 'imdb', 'imdb')
    con.execute(f"EXPORT DATABASE '{export_path}' (FORMAT PARQUET, OVERWRITE TRUE)")

    schemas_path = os.path.join(export_path, 'schema.sql')
    schemas_file = open(schemas_path, 'r')
    creates = schemas_file.read()
    schemas_file.close()
    creates = creates.split(';')
    schema_queries = [create.strip() for create in creates if create.strip()]

    job_queries_dir = os.path.join(SQL_STORM_REPO_DIR, 'v1.0', 'job', 'queries')
    sql_files = [f for f in os.listdir(job_queries_dir) if f.endswith('.sql')]
    benchmark_queries = [
        open(os.path.join(job_queries_dir, sql_file), 'r').read() for sql_file in sql_files
    ]

    benchmark_name = 'sql-storm-imdb'
    repo_data = RepoData.from_queries(benchmark_name, benchmark_queries, schema_queries, export_path, '.parquet')
    con = get_con()
    add_3rd_party(con, repo_data, replace_existing=True)




def main():
    """Main function to download and import StackOverflow data."""
    add_sql_storm_stack_overflow()
    add_sql_storm_tpc('tpc-h')
    add_sql_storm_tpc('tpc-ds')
    add_sql_storm_job()


if __name__ == "__main__":
    main()
    print("SqlStorm data has been downloaded and extracted.")
