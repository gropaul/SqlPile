import os

from src.config import QUERIES_DIR, DATA_DIR

CLUSTER_DATA_PATH = '/export/scratch2/home/gross/SqlPile/data/'
REMOTE_NAME = 'gross@diamonds4'

queries_dir_from_data_path = QUERIES_DIR.replace(DATA_DIR + '/', '')
CLUSTER_QUERIES_DIR = os.path.join(CLUSTER_DATA_PATH, queries_dir_from_data_path)

LOCAL_QUERIES_DIR = QUERIES_DIR
CLUSTER_UNIFIED_FILE_PATH = os.path.join(CLUSTER_QUERIES_DIR, 'parquet_queries_tmp.parquet')
LOCAL_UNIFIED_FILE_PATH = os.path.join(LOCAL_QUERIES_DIR, '_unified', 'parquet_queries_tmp.parquet')

def load_queries_from_cluster():



    print(f"Copying unified from {CLUSTER_UNIFIED_FILE_PATH} to {LOCAL_UNIFIED_FILE_PATH}")

    # use scp to copy the unified queries from the cluster to the local machine
    command = f'scp -r {REMOTE_NAME}:{CLUSTER_UNIFIED_FILE_PATH} {LOCAL_UNIFIED_FILE_PATH}'
    print(f"Running command: {command}")
    # os.system(f'scp -r {REMOTE_NAME}:{CLUSTER_QUERIES_DIR} {LOCAL_QUERIES_DIR}')
    print("Queries copied successfully.")


def unifiy_file():
    from_query = f"SELECT * FROM '{CLUSTER_QUERIES_DIR}/*/*.parquet'"

    # copy to parquet_queries_tmp (COPY (SELECT * FROM tbl) TO 'output.parquet' (FORMAT parquet);)
    copy_quey = f""" 
    COPY ({from_query}) TO '{CLUSTER_UNIFIED_FILE_PATH}' (FORMAT parquet);
    """

    print(f"Running query: \n{copy_quey}")


if __name__ == "__main__":
    unifiy_file()
    load_queries_from_cluster()
