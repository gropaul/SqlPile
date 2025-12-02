from typing import Literal

from src.config import KAGGLE_DATA_DB_PATH, HUGGINFACE_DATA_DB_PATH, ROOT

DataSet = Literal['kaggle', 'huggingface']

CLUSTER_ROOT = '/export/scratch2/home/gross/SqlPile'
CLUSTER_NAME = 'diamonds4'

def download_from_cluster(dataset: DataSet) -> str:
    local_path = ''

    if dataset == 'kaggle':
        local_path = KAGGLE_DATA_DB_PATH
    else:
        local_path = HUGGINFACE_DATA_DB_PATH

    base_path = ROOT
    remote_path = local_path.replace(base_path, CLUSTER_ROOT)

    # use scp to download the file
    cmd = f"scp gross@{CLUSTER_NAME}:{remote_path} {local_path}"
    print(cmd)


if __name__ == "__main__":
    download_from_cluster('kaggle')