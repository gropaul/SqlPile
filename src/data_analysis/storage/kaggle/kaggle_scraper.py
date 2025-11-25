import kaggle
import duckdb
from time import sleep
import requests

from tqdm import tqdm

from src.config import KAGGLE_DATASETS_DB_PATH
import site, os





python_path = os.path.join(site.getuserbase(), 'bin')
kaggle.api.authenticate()
KAGGLE_API_BINARY = os.path.join(python_path, 'kaggle')


def download_dataset(dataset: str, unzip: bool = True):
    files = kaggle.api.dataset_list_files(dataset)

    return
    # path = 'data/' + dataset.split('/')[1]
    # kaggle.api.dataset_download_files(dataset, path=path, unzip=unzip)


def scrape_files(con: duckdb.DuckDBPyConnection):
    datasets = con.execute("SELECT ref FROM kaggle_datasets WHERE ref NOT IN (SELECT DISTINCT dataset_ref FROM kaggle_dataset_files)").fetchall()
    for (dataset_ref,) in tqdm(datasets, desc="Datasets", unit="dataset"):
        try:
            sleep(1)
            store_dataset_files(con, dataset_ref)
        except Exception as e:
            print(f"Error fetching files for dataset {dataset_ref}: {e}")
            sleep(30)


def store_dataset_files(con: duckdb.DuckDBPyConnection, dataset_ref: str):
    files = kaggle.api.dataset_list_files(dataset_ref, page_size=1000)
    for file in files.files:
        con.execute("""
            INSERT INTO kaggle_dataset_files (dataset_ref, file_name, total_bytes)
            VALUES (?, ?, ?)
        """, (
            dataset_ref,
            file.name,
            file.total_bytes
        ))

def create_schema(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE TABLE IF NOT EXISTS kaggle_datasets (
            ref TEXT,
            title TEXT,
            total_bytes INT64,
            download_count INT64,
            vote_count INT64,
            license_name TEXT,
            page INT64,
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS kaggle_dataset_files (
            dataset_ref TEXT,
            file_name TEXT,
            total_bytes INT64
        )
    """)

    con.execute("""
        INSTALL shellfs FROM community;
        LOAD shellfs;
    """)
def scrape_for_datasets_v1(con: duckdb.DuckDBPyConnection, file_type: str = "parquet", max_pages: int = 100):

    gb_to_bytes = 1024 * 1024 * 1024
    for page in tqdm(range(1, max_pages + 1), desc="Pages", unit="page"):
        try:
            url = f"https://www.kaggle.com/api/v1/datasets/list?group=public&sortby=votes&size=all&filetype={file_type}&license=all&viewed=unspecified&minsize=536870912&page={page}"
            # use python's requests library to get the data
            response = requests.get(url)
            if response.status_code != 200:
                print(f"Error fetching datasets on page {page}: {response.status_code}")
                sleep(30)
                continue

            body = response.json()
            for dataset in body:
                # check if dataset already exists
                res = con.execute("SELECT COUNT(*) FROM kaggle_datasets WHERE ref = ?", (dataset['ref'],)).fetchone()
                if res[0] > 0:
                    continue
                # insert dataset information into the database
                # dataset is as json object
                con.execute("""
                        INSERT INTO kaggle_datasets (ref, title, total_bytes, download_count, vote_count, license_name, page)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                    dataset['ref'],
                    dataset['title'],
                    dataset['totalBytes'],
                    dataset['downloadCount'],
                    dataset['voteCount'],
                    dataset['licenseName'],
                    page
                ))

        except Exception as e:
            print(f"Error fetching datasets on page {page}: {e}")
            sleep(2)


def scrape_for_datasets_v2(con: duckdb.DuckDBPyConnection, file_type: str, max_pages: int = 50):


        for page in range(1, max_pages + 1):
            cmd = f"{KAGGLE_API_BINARY} datasets list --file-type {file_type} --csv --page {page}  --sort-by votes"
            try:
                query = f"""
                       INSERT INTO kaggle_datasets (
                           SELECT ref, title, size AS total_bytes, downloadCount AS download_count, voteCount AS vote_count, null AS license_name, {page} AS page
                           FROM read_csv('{cmd} |')
                           WHERE ref NOT IN (SELECT ref FROM kaggle_datasets)
                       )
                   """
                con.execute(query)
                print(f"Scraped page {page} for file type {file_type}")
            except Exception as e:
                print(f"Error scraping datasets for file type {file_type} on page {page}: {e}")
                if "No datasets found" in str(e):
                    return




def retrieve_kaggle_datasets():
    # execute "pip install kaggle"
    if not os.path.exists(KAGGLE_API_BINARY):
        os.system("pip install kaggle --user")

    con = duckdb.connect(KAGGLE_DATASETS_DB_PATH)
    create_schema(con)
    scrape_for_datasets_v2(con, "sqlite", max_pages=30)
    scrape_for_datasets_v1(con, "parquet", max_pages=200)
    scrape_files(con)


if __name__ == "__main__":
    retrieve_kaggle_datasets()


