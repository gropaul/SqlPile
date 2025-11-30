from src.config import KAGGLE_DATA_DB_PATH
from src.data_analysis.storage.dataset_adapter import DatasetAdapter


def save_kaggle_in_database():
    """
    This adds kaggle repositories to the database, as well as
    a) tables
    b) columns
    c) table_sizes
    d) string_data
    Each dataset is its own repository
    """
    adapter = DatasetAdapter(
        source_db_path=KAGGLE_DATA_DB_PATH,
        source_db_alias='kaggle_data',
        repo_prefix='kaggle',
    )

    adapter.connect()
    adapter.import_all_schemas()
    adapter.close()


def delte_kaggle_repos():
    """
    Delete all kaggle repositories from the main database.
    """
    adapter = DatasetAdapter(
        source_db_path=KAGGLE_DATA_DB_PATH,
        source_db_alias='kaggle_data',
        repo_prefix='kaggle',
    )

    adapter.connect()
    adapter.delete_repos_by_prefix()
    adapter.close()


if __name__ == "__main__":
    delte_kaggle_repos()
    save_kaggle_in_database()
