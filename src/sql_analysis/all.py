from src.remote.retrieve_queries import download_data_from_remote
from src.remote.split_file import partition_data
from src.sql_analysis.add_3rd_party.add_sql_storm import add_sql_storm_stack_overflow
from src.sql_analysis.add_3rd_party.add_tpc import add_tpc
from src.sql_analysis.execute_queries import execute_repo_queries
from src.sql_analysis.get_schemas_from_create_query import get_schemas_from_create_query
from src.sql_analysis.load_queries_to_database import load_queries_to_database
from src.sql_analysis.load_schemapile_json_to_ddb import load_schemapile_json_to_database


def all(download: bool = True) -> None:

    if download:
        download_data_from_remote()
        partition_data(chunk_size=1000)

    # load_schemapile_json_to_database(ask=False)
    # load_queries_to_database(ask=False)
    # add_tpc('tpc-h')
    # add_tpc('tpc-ds')
    # add_sql_storm_stack_overflow()
    # get_schemas_from_create_query()
    execute_repo_queries()



if __name__ == "__main__":
    all(download=False)