from src.config import QUERIES_DIR_PARTITIONED, QUERIES_DIR_RAW
from src.remote.retrieve_queries import download_data_from_remote
from src.remote.split_file import partition_data
from src.sql_analysis.add_3rd_party.add_all_3rd_paries import add_all_benchmarks
from src.sql_analysis.execute_queries import execute_repo_queries
from src.sql_analysis.get_schemas_from_create_query import get_schemas_from_create_query
from src.sql_analysis.load_queries_to_database import load_queries_to_database
from src.sql_analysis.load_schemapile_json_to_ddb import load_schemapile_json_to_database


def all(download: bool = True, partition: bool = True):

    if download:
        download_data_from_remote()
        partition_data(chunk_size=1000)

    if partition:
        queries_path = f'{QUERIES_DIR_PARTITIONED}/*/*.parquet'
    else:
        queries_path = f'{QUERIES_DIR_RAW}/*/*.parquet'

    load_schemapile_json_to_database(ask=False)
    load_queries_to_database(ask=False, source_path=queries_path)
    add_all_benchmarks()
    get_schemas_from_create_query()



if __name__ == "__main__":
    all(download=False, partition=False)