import os

from src.config import HUGGINFACE_DATA_DB_PATH, KAGGLE_DATA_DB_PATH, DATABASE_PATH
from src.data_analysis.storage.huggingface.download_data import format_bytes


def print_file_size(path: str) -> None:
    size_bytes = os.path.getsize(path)
    bytes_string = format_bytes(size_bytes)
    print(f"File: {path}, Size: {bytes_string}")


def print_files_sizes(paths: list[str]) -> None:
    for path in paths:
        print_file_size(path)



if __name__ == "__main__":
    paths = [
        HUGGINFACE_DATA_DB_PATH,
        KAGGLE_DATA_DB_PATH,
        DATABASE_PATH
    ]

    print_files_sizes(paths)