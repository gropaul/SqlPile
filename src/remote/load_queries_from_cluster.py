import os

from src.config import QUERIES_DIR_RAW, DATA_DIR, QUERIES_DIR_FROM_CLUSTER

CLUSTER_DATA_PATH = '/export/scratch2/home/gross/SqlPile/data/'
REMOTE_NAME = 'gross@diamonds4'

queries_dir_from_data_path = QUERIES_DIR_RAW.replace(DATA_DIR + '/', '')
CLUSTER_QUERIES_DIR = os.path.join(CLUSTER_DATA_PATH, queries_dir_from_data_path)

LOCAL_QUERIES_DIR = QUERIES_DIR_RAW
CLUSTER_UNIFIED_FILE_PATH = os.path.join(CLUSTER_QUERIES_DIR, 'parquet_queries_tmp.parquet')
QUERIES_DIAMOND4_PATH = os.path.join(QUERIES_DIR_FROM_CLUSTER, 'queries_diamonds4.parquet')

def load_queries_from_cluster():

    print(f"Copying unified from {CLUSTER_UNIFIED_FILE_PATH} to {QUERIES_DIAMOND4_PATH}")

    # use scp to copy the unified queries from the cluster to the local machine
    command = f'scp -r {REMOTE_NAME}:{CLUSTER_UNIFIED_FILE_PATH} {QUERIES_DIAMOND4_PATH}'
    print(f"Running command: {command}")
    # os.system(f'scp -r {REMOTE_NAME}:{CLUSTER_QUERIES_DIR} {LOCAL_QUERIES_DIR}')
    print("Queries copied successfully.")


def unifiy_file():
    from_query = f"SELECT * FROM '{CLUSTER_QUERIES_DIR}/*/*.parquet'"

    # copy to parquet_queries_tmp (COPY (SELECT * FROM tbl) TO 'output.parquet' (FORMAT parquet);)
    copy_quey = f""" 
    COPY ({from_query}) TO '{CLUSTER_UNIFIED_FILE_PATH}' (FORMAT parquet, OVERWRITE TRUE, COMPRESSION ZSTD);
    """

    import duckdb
    con = duckdb.connect()
    print(f"Creating unified file at {CLUSTER_UNIFIED_FILE_PATH}")
    con.execute(copy_quey)





if __name__ == "__main__":
    unifiy_file()
    load_queries_from_cluster()
