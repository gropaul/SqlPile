import os
from typing import List

import requests

from src.data_analysis.storage.huggingface.models import ParquetFile


def get_file_infos(id: str) -> List[ParquetFile]:
    API_TOKEN = os.environ.get("HF_TOKEN", "")
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    API_URL = f"https://datasets-server.huggingface.co/parquet?dataset={id}"

    response = requests.get(API_URL, headers=headers)
    parsed_files = []
    for file in response.json()['parquet_files']:
        parsed_file = ParquetFile(
            id=file['dataset'],
            config=file['config'],
            split=file['split'],
            path=file['url'],
            size_bytes=file['size']
        )
        parsed_files.append(parsed_file)

    return parsed_files


if __name__ == "__main__":
    get_file_infos('lavita/medical-qa-shared-task-v1-toy')
