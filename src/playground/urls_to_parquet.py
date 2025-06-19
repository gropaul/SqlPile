import json
import os

from tqdm import tqdm

from src.sql_scraping.analyse_repo import get_repo_name_and_url

URLS_JSON_PATH = "/Users/paul/workspace/SqlPile/data/urls_and_licenses.json"


class Repo:

    def __init__(self, name: str, url: str, license: str, permissive: bool):
        self.name = name
        self.url = url
        self.license = license
        self.permissive = permissive

    def __repr__(self):
        return f"Repo(name={self.name}, url={self.url}, license={self.license}, permissive={self.permissive})"

    def __eq__(self, other):
        if not isinstance(other, Repo):
            return False
        return (self.name == other.name and
                self.url == other.url and
                self.license == other.license and
                self.permissive == other.permissive)

    def __hash__(self):
        return hash(self.url)

    def to_dict(self):
        return {
            "name": self.name,
            "url": self.url,
            "license": self.license,
            "permissive": self.permissive
        }


def urls_to_parquet():

    json_data = json.load(open(URLS_JSON_PATH, "r"))

    repos = {}
    n_added = 0
    n_refused = 0

    with tqdm(json_data, desc="Processing repositories", unit="repo") as pbar:
        for file in pbar:

            file_entry = json_data[file]
            info = file_entry["INFO"]

            file_url = info["URL"]
            repo_name, repo_url = get_repo_name_and_url(file_url)

            license = info["LICENSE"]
            permissive = info["PERMISSIVE"]

            repo = Repo(name=repo_name, url=repo_url, license=license, permissive=permissive)

            # check if the repo is already in the list
            if repo.url not in repos:
                repos[repo.url] = repo
                n_added += 1
            else:
                n_refused += 1

            pbar.set_postfix(
                {
                    "added": n_added,
                    "refused": n_refused,
                }
            )

        # add info to tqdm

    print(f"Found {len(repos)} unique repositories.")

    # Save to Parquet file, use compression for smaller file size
    import pandas as pd
    df = pd.DataFrame([repo.to_dict() for repo in repos.values()])
    parquet_path = "/Users/paul/workspace/SqlPile/input_data/repos.parquet"


    # Compress the file as it will be committed to the repository
    df.to_parquet(parquet_path, index=False, compression='BROTLI')

    # tell the file size
    file_size = os.path.getsize(parquet_path) / (1024 * 1024)  # size in MB
    print(f"DataFrame size: {file_size:.2f} MB")

    print(f"Saved repositories to {parquet_path}")




if __name__ == "__main__":
    urls_to_parquet()