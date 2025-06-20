import os

from src.config import QUERIES_DIR, DATA_DIR


def load_queries_from_cluster():

    CLUSTER_DATA_PATH = '/export/scratch2/home/gross/SqlPile/data/'
    REMOTE_NAME = 'gross@diamonds4'

    queries_dir_from_data_path = QUERIES_DIR.replace(DATA_DIR + '/', '')
    CLUSTER_QUERIES_DIR = os.path.join(CLUSTER_DATA_PATH, queries_dir_from_data_path)

    LOCAL_QUERIES_DIR =  QUERIES_DIR

    print(f"Copying queries from {CLUSTER_QUERIES_DIR} to {LOCAL_QUERIES_DIR}")

    # use scp to copy the queries from the cluster to the local machine
    command = f'scp -r {REMOTE_NAME}:{CLUSTER_QUERIES_DIR} {LOCAL_QUERIES_DIR}'
    print(f"Running command: {command}")
    # os.system(f'scp -r {REMOTE_NAME}:{CLUSTER_QUERIES_DIR} {LOCAL_QUERIES_DIR}')
    print("Queries copied successfully.")


if __name__ == "__main__":
    load_queries_from_cluster()