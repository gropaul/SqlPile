from src.config import HUGGINFACE_DATA_DB_PATH
from src.data_analysis.storage.dataset_adapter import DatasetAdapter
from src.sql_analysis.utils.names import clean_name


def save_huggingface_in_database():
    """
    This adds huggingface repositories to the database, as well as
    a) tables
    b) columns
    c) table_sizes
    d) string_data
    Each dataset is its own repository
    """
    adapter = DatasetAdapter(
        source_db_path=HUGGINFACE_DATA_DB_PATH,
        source_db_alias='huggingface_data',
        repo_prefix='huggingface',
    )

    adapter.connect()
    adapter.import_all_schemas()
    adapter.close()


def delete_huggingface_repos():
    """
    Delete all huggingface repositories from the main database.
    """
    adapter = DatasetAdapter(
        source_db_path=HUGGINFACE_DATA_DB_PATH,
        source_db_alias='huggingface_data',
        repo_prefix='huggingface',
    )

    adapter.connect()
    adapter.delete_repos_by_prefix()
    adapter.close()


if __name__ == "__main__":
    delete_repos = input("Do you want to delete all existing huggingface repositories from the database? (y/n): ")
    if delete_repos.lower() == 'y':
        delete_huggingface_repos()
    save_huggingface_in_database()
