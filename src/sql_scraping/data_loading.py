import json
import os
from typing import List
import duckdb
from src.config import DATA_DIR, logger, QUERIES_DIR

from src.config import ROOT
from src.sql_scraping.analyse_repo import get_repo_name_and_url
from src.sql_scraping.extract_sql import get_dir_for_url


def read_schemapile_data():
    """Read the JSON data from schemapile-perm.json file."""
    # read the json schemapile-perm.json file
    path = os.path.join(DATA_DIR, 'schemapile-perm.json')
    with open(path, 'r') as file:
        data = json.load(file)
    return data


def get_urls(filter_analysed: bool, shuffle: bool = False) -> List[str]:
    data = read_schemapile_data()

    urls = []
    url_storage_dirs = []
    for item in data:
        value = data[item]
        # Get the repository URL
        file_path = value['INFO']['URL']
        file_path = file_path.strip()  # Clean up the URL
        name, url = get_repo_name_and_url(file_path)
        storage_dir_path = get_dir_for_url(url)
        dir_name = os.path.basename(storage_dir_path)

        # only add the URL if it is not already in the list
        if url not in urls:
            urls.append(url)
            url_storage_dirs.append(dir_name)

    if filter_analysed:
        print('Filtering out already analysed URLs...')
        n_removed = 0
        # list folder names in the queries directory

        queries_dir = os.path.join(DATA_DIR, QUERIES_DIR)
        existing_repos = os.listdir(queries_dir)
        existing_repos = [repo.replace('.zip', '') for repo in existing_repos]

        for (url, url_dir_name) in zip(urls, url_storage_dirs):

            if url_dir_name in existing_repos:
                urls.remove(url)
                n_removed += 1

        logger.info(f"Removed {n_removed} URLs that were already analysed.")
        logger.info(f"Existing repositories: {len(existing_repos)}")
        logger.info(f"Total URLs to process: {len(urls)}")

    if shuffle:
        import random
        random.shuffle(urls)
        logger.info("Shuffled the URLs.")
    return urls
