import kaggle
import duckdb
from time import sleep
from IPython.core.page import page_dumb
from datasets import tqdm

from src.config import KAGGLE_DATASETS_DB_PATH

kaggle.api.authenticate()


# create dataset table
con = duckdb.connect(KAGGLE_DATASETS_DB_PATH)

con.execute("""
    CREATE TABLE IF NOT EXISTS kaggle_datasets (
        id INT64, 
        ref TEXT,
        title TEXT,
        subtitle TEXT,
        url TEXT,
        total_bytes INT64,
        download_count INT64,
        view_count INT64,
        vote_count INT64,
        license_name TEXT,
        page INT64,
    )
""")

con.execute("""
    CREATE TABLE  IF NOT EXISTS kaggle_dataset_files (
        dataset_id INT64,
        file_name TEXT,
        total_bytes INT64
    )
""")

def download_dataset(dataset: str, unzip: bool = True):
    files = kaggle.api.dataset_list_files(dataset)

    return
    # path = 'data/' + dataset.split('/')[1]
    # kaggle.api.dataset_download_files(dataset, path=path, unzip=unzip)


def store_dataset_information(con: duckdb.DuckDBPyConnection, dataset: any, page: int):

    # check if dataset already exists
    res = con.execute("SELECT COUNT(*) FROM kaggle_datasets WHERE id = ?", (dataset.id,)).fetchone()
    if res[0] > 0:
        print(f"Dataset {dataset.ref} already exists in the database. Skipping.")
        return

    # insert dataset information into the database

    # dataset is as json object
    con.execute("""
        INSERT INTO kaggle_datasets (id, ref, title, subtitle, url, total_bytes, download_count, view_count, vote_count, license_name, page)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        dataset.id,
        dataset.ref,
        dataset.title,
        dataset.subtitle,
        dataset.url,
        dataset.total_bytes,
        dataset.download_count,
        dataset.view_count,
        dataset.vote_count,
        dataset.license_name,
        page
    ))

    # sleep for 1 second to avoid rate limiting
    sleep(1)
    files = kaggle.api.dataset_list_files(dataset.ref, page_size=1000)
    for file in files.files:
        con.execute("""
            INSERT INTO kaggle_dataset_files (dataset_id, file_name, total_bytes)
            VALUES (?, ?, ?)
        """, (
            dataset.id,
            file.name,
            file.total_bytes
        ))


try:
    max_page = con.execute("SELECT MAX(page) FROM kaggle_datasets").fetchone()[0]
    if max_page is None:
        max_page = 0
except Exception as e:
    max_page = 0

gb_to_bytes = 1024 * 1024 * 1024
for page in tqdm(range(max_page + 1, 10_000), desc="Pages", unit="page"):
    datasets = kaggle.api.dataset_list(
        sort_by="votes",
        search="parquet",
        page=page,
        min_size= 1 * gb_to_bytes,  # at least 1 GB
    )

    if len(datasets) == 0:
        print(f"No more datasets found at page {page}. Stopping.")
        break

    for d in datasets:
        store_dataset_information(con, d, page)