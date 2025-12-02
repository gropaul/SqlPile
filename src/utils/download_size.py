import os

from src.config import HUGGINFACE_DATA_DB_PATH, KAGGLE_DATA_DB_PATH, DATABASE_PATH
from src.data_analysis.storage.huggingface.download_data import format_bytes


def print_file_size(path: str) -> None:

    # check if the file exists
    if not os.path.exists(path):
        print(f"File: {path} does not exist.")
        return

    size_bytes = os.path.getsize(path)
    bytes_string = format_bytes(size_bytes)
    print(f"File: {path}, Size: {bytes_string}")


def get_stats(duckdb_path: str):
    if not os.path.exists(duckdb_path):
        print(f"File: {duckdb_path} does not exist.")
        return {}

    # connect to the duckdb database, get the number of schemas, tables, and columns
    import duckdb
    con = duckdb.connect(duckdb_path, read_only=True)
    n_schemas = con.execute("SELECT COUNT(DISTINCT table_schema) FROM information_schema.tables").fetchone()[0]
    n_tables = con.execute("SELECT COUNT(*) FROM information_schema.tables").fetchone()[0]
    n_columns = con.execute("SELECT COUNT(*) FROM information_schema.columns").fetchone()[0]
    print(f"Database: {duckdb_path}, Schemas: {n_schemas}, Tables: {n_tables}, Columns: {n_columns}")



if __name__ == "__main__":
    paths = [
        HUGGINFACE_DATA_DB_PATH,
        KAGGLE_DATA_DB_PATH,
        DATABASE_PATH
    ]

    for path in paths:
        get_stats(path)
        print_file_size(path)