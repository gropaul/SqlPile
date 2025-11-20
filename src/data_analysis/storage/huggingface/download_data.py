import os

from tqdm import tqdm
import duckdb
from src.config import HUGGINFACE_DATASETS_DB_PATH, HUGGINFACE_DATA_DB_PATH

LIMIT = 128000 * 4


def download_data():

    dataset_con = duckdb.connect(HUGGINFACE_DATASETS_DB_PATH, read_only=True)
    data_con = duckdb.connect(HUGGINFACE_DATA_DB_PATH)

    hf_token = os.environ.get("HF_TOKEN", "")
    query = f"CREATE SECRET HF_TOKEN (TYPE HUGGINGFACE, TOKEN '{hf_token}');"
    print(query)
    data_con.execute(query)

    datasets = dataset_con.execute("SELECT parse_result_id, config_name, split, MIN(path) FROM data_files GROUP BY ALL ORDER BY ALL;").fetchall()

    for (dataset_id, config_name, split, path) in tqdm(datasets, desc="Downloading datasets", unit="dataset"):
        print(f"Downloading dataset: {dataset_id}, config: {config_name}, split: {split}, path: {path}")
        query = f"""CREATE OR REPLACE TABLE "{dataset_id}__{config_name}__{split}" AS SELECT * FROM read_parquet('{path}') LIMIT {LIMIT};"""
        print(query)
        data_con.execute(query)
        data_con.execute("CHECKPOINT;")


if __name__ == "__main__":
    download_data()
