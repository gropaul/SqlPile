import json
import os
from typing import List

from config import DATA_DIR, logger


def read_data():
    """Read the JSON data from schemapile-perm.json file."""
    # read the json schemapile-perm.json file
    path = os.path.join(DATA_DIR, 'schemapile-perm.json')
    with open(path, 'r') as file:
        data = json.load(file)
    return data


def get_urls() -> List[str]:
    data = read_data()

    # check if there is already a urls.csv file
    csv_path = os.path.join(DATA_DIR, 'schemapile-perm-url.csv')

    if os.path.exists(csv_path):
        logger.info(f"URLs already extracted in {csv_path}. Skipping extraction.")
        # read the URLs from the csv file
        with open(csv_path, 'r') as file:
            urls = [line.strip() for line in file.readlines()]
        return urls

    urls = []
    for item in data:
        value = data[item]
        # Get the repository URL
        url = value['INFO']['URL']
        url = url.strip()  # Clean up the URL
        urls.append(url)

    # Save the URLs to a CSV file
    with open(csv_path, 'w') as file:
        for url in urls:
            file.write(f"{url}\n")

    return urls
