import os
import time
import urllib.request
import tempfile
import ssl
from pathlib import Path

from tqdm import tqdm
import duckdb

from src.config import HUGGINFACE_DATASETS_DB_PATH, HUGGINFACE_DATA_DB_PATH, MAX_VALUES_TO_ANALYZE_PER_COLUMN, \
    HUGGINFACE_DATASETS_CPY_DB_PATH

LIMIT = MAX_VALUES_TO_ANALYZE_PER_COLUMN * 2

# clear temp download dir

DOWNLOAD_DIR = os.path.join(tempfile.gettempdir(), "huggingface_parquet_downloads")


def execute_query_with_retries(con: duckdb.DuckDBPyConnection, query: str, max_retries: int = 3, sleep_seconds: int = 2):
    retries = 0
    while retries < max_retries:
        try:
            con.execute(query)
            return
        except Exception as e:
            print(f"Error executing query: {e}. Retrying {retries + 1}/{max_retries}...")
            time.sleep(sleep_seconds)
            retries += 1
    raise Exception(f"Failed to execute query after {max_retries} retries.")

import urllib.request
import os
import shutil
from pathlib import Path
import urllib.request
import ssl
import math

def format_bytes(size_bytes: int) -> str:
    if size_bytes == 0:
        return "0B"

    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)

    return f"{s} {size_name[i]}"


def download_parquet_file(url: str, local_dir: str, hf_token: str = "") -> str:
    local_dir = Path(local_dir)

    # Always clear the directory (no caching)
    if local_dir.exists():
        shutil.rmtree(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)

    filename = url.split("/")[-1].split("?")[0]
    local_path = local_dir / filename

    print(f"\tDownloading: {url}")
    print(f"\t→ {local_path}")

    # Request setup
    request = urllib.request.Request(url)
    if hf_token:
        request.add_header("Authorization", f"Bearer {hf_token}")

    # SSL context (permissive)
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    try:
        with urllib.request.urlopen(request, context=ssl_context) as response:
            total_size = response.getheader("Content-Length")
            total_size = int(total_size) if total_size else None

            downloaded = 0
            block_size = 8192

            with open(local_path, "wb") as f:
                while True:
                    chunk = response.read(block_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)

                    if total_size:
                        percent = downloaded / total_size * 100
                        downloaded_fmt = format_bytes(downloaded)
                        total_size_fmt = format_bytes(total_size)
                        print(f"\r\tProgress: {percent:6.2f}% ({downloaded_fmt} / {total_size_fmt})", end="")
                    else:
                        print(f"\r\tDownloaded: {downloaded} bytes", end="")

        print("\n\tDownload complete.")
        return str(local_path)

    except Exception as e:
        print(f"\tDownload failed: {e}")
        raise



def download_data(download_locally: bool = False):
    """
    Downloads data from HuggingFace datasets into DuckDB.

    Args:
        download_locally: If True, downloads parquet files to local storage first before importing.
                         If False (default), imports directly from remote URLs.
    """

    dataset_con = duckdb.connect(HUGGINFACE_DATASETS_CPY_DB_PATH, read_only=True)
    data_con = duckdb.connect(HUGGINFACE_DATA_DB_PATH)

    hf_token = os.environ.get("HF_TOKEN", "")

    # Only create secrets if not downloading locally (needed for direct import)
    if not download_locally:
        data_con.execute(f"CREATE SECRET HF_TOKEN  (TYPE HUGGINGFACE, TOKEN '{hf_token}');")
        data_con.execute(f"CREATE SECRET HTTP_AUTH (TYPE HTTP, BEARER_TOKEN '{hf_token}');")

    # get the table_names that are already downloaded from the schema of data_con
    existing_tables_df = data_con.execute("""
        SELECT table_name
        FROM information_schema.tables
    """).fetchdf()

    datasets = dataset_con.execute(f"""
        WITH config_split_sizes AS (
            SELECT parse_result_id, config_name, split, SUM(size_bytes) as total_bytes, list(path ORDER BY path) AS paths
            FROM parquet_files 
            GROUP BY ALL 
            ORDER BY ALL
        ),
        biggest_splits AS (
            SELECT parse_result_id, first(paths ORDER BY total_bytes DESC) AS biggest_split_paths
            FROM config_split_sizes
            GROUP BY parse_result_id
        )
        SELECT parse_result_id, biggest_split_paths AS paths
        FROM biggest_splits
        GROUP BY ALL ORDER BY ALL;
    """).fetchall()

    for (dataset_id, paths) in tqdm(datasets):
        print(f"Downloading dataset: {dataset_id}, num files: {len(paths)}")
        table_name = f"{dataset_id.replace('/', '_')}"
        rows_remaining = LIMIT

        if table_name in existing_tables_df['table_name'].values:
            print(f"Dataset {dataset_id} already downloaded, skipping.")
            continue

        for (i, path) in enumerate(paths):
            if rows_remaining <= 0:
                break

            print(f"\tProcessing file: {path}, rows remaining: {rows_remaining}")

            # Determine the path to use (local or remote)
            if download_locally:
                # Download the file locally first
                local_path = download_parquet_file(path, DOWNLOAD_DIR, hf_token)
                parquet_path = local_path
            else:
                # Use the remote URL directly
                parquet_path = path

            if i == 0:
                file_query = f"""
                    CREATE TABLE "{table_name}" AS
                    SELECT *
                    FROM read_parquet('{parquet_path}')
                    -- LIMIT {rows_remaining};
                """
            else :
                file_query = f"""
                    INSERT INTO "{table_name}"
                    SELECT *
                    FROM read_parquet('{parquet_path}')
                    -- LIMIT {rows_remaining};
                """

            execute_query_with_retries(data_con, file_query)
            # get the number of rows inserted
            rows_inserted = data_con.execute(f'SELECT COUNT(*) FROM "{table_name}";').fetchone()[0]
            rows_remaining = LIMIT - rows_inserted




if __name__ == "__main__":
    download_data(download_locally=True)
