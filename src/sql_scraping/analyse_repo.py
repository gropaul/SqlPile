import os
import subprocess
from typing import Tuple, Optional, List

import requests
from tqdm import tqdm

from src.config import REPO_DIR, logger, DELETE_REPOS_AFTER_ANALYSIS, PROCESS_ZIPPED_REPOS
from src.sql_scraping.extract_sql import RepoAnalysisResult, FileAnalysisResult, extract_sql_from_repo


def get_repo_name_and_url(repo_path) -> Tuple[str, str]:
    # make sure the repo_path is a valid GitHub URL
    if not repo_path.startswith("https://github.com"):
        raise ValueError("Invalid GitHub repository URL")

    # remove https
    repo_path = repo_path.replace("https://", "")
    # split the URL to get the repo name and URL
    parts = repo_path.split('/')
    username = parts[1]
    repo_name = parts[2]
    repo_url = f"https://github.com/{username}/{repo_name}"

    return repo_name, repo_url

def is_repo_active(repo_url: str) -> bool:
    try:
        response = requests.get(repo_url)
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        else:
            return False
    except requests.RequestException as e:
        return False

def get_or_clone_repo(repo_url: str, repo_name: str) -> Optional[str]:
    """Clone the repository if it doesn't already exist locally."""
    target_path = os.path.join(REPO_DIR, repo_name)
    try:
        if os.path.exists(target_path):
            logger.info(f"Repository {repo_name} already exists at {target_path}.")
            return target_path

        # check if the repository exists as a zip file
        zip_path = os.path.join(REPO_DIR, f"{repo_name}.zip")
        if os.path.exists(zip_path):
            if PROCESS_ZIPPED_REPOS:
                logger.info(f"Repository {repo_name} found as a zip file at {zip_path}. Unzipping...")
                subprocess.run(["unzip", zip_path, "-d", REPO_DIR], check=True,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                os.remove(zip_path)
                logger.info(f"Unzipped repository {repo_name} successfully. Deleting zip file {zip_path}.")
            else:
                logger.info(f"Repository {repo_name} found as a zip file but will be skipped due to PROCESS_ZIPPED_REPOS being False.")
                return None

        logger.info(f"Cloning repository {repo_name} from {repo_url}...")
        subprocess.run(["git", "clone", repo_url, target_path], check=True)

        logger.info(f"Repository {repo_name} retrieved successfully.")
        return target_path
    except subprocess.CalledProcessError as e:
        logger.error(f"Error cloning repository: {e}")
    except Exception as e:
        logger.error(f"Unexpected error while cloning repository: {e}")

    return None

def delete_repo(repo_name: str) -> None:
    """Delete the cloned repository directory. This is useful for cleanup after analysis."""
    target_path = os.path.join(REPO_DIR, repo_name)
    if os.path.exists(target_path):
        try:
            logger.info(f"Deleting repository {repo_name} at {target_path}...")
            subprocess.run(["rm", "-rf", target_path], check=True)
            logger.info(f"Repository {repo_name} deleted successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error deleting repository: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while deleting repository: {e}")
    else:
        logger.warning(f"Repository {repo_name} does not exist at {target_path}. No action taken.")


import os
import subprocess
from typing import Optional

def get_dir_size(path: str) -> int:
    """Returns the total size of all files in the given directory."""
    total = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total += os.path.getsize(fp)
    return total

def compress_repo(repo_name: str) -> Optional[str]:
    """Compresses the cloned repository into a zip file. Delete the original directory after compression."""
    target_path = os.path.join(REPO_DIR, repo_name)

    # make sure it is a directory
    if not os.path.isdir(target_path):
        logger.error(f"Target path {target_path} is not a directory. Cannot compress.")
        return None
    zip_path = os.path.join(REPO_DIR, f"{repo_name}.zip")
    try:
        if not os.path.exists(target_path):
            logger.error(f"Repository {repo_name} does not exist at {target_path}. Cannot compress.")
            return None

        original_size = get_dir_size(target_path)
        logger.info(f"Original size of {repo_name}: {original_size / 1024:.2f} KB")

        subprocess.run(["zip", "-r", zip_path, target_path], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if os.path.exists(zip_path):
            zip_size = os.path.getsize(zip_path)
            logger.info(f"Compressed size of {repo_name}.zip: {zip_size / 1024:.2f} KB")

        logger.info(f"Repository {repo_name} compressed successfully.")

        delete_repo(repo_name)
        return zip_path
    except subprocess.CalledProcessError as e:
        logger.error(f"Error compressing repository: {e}")
    except Exception as e:
        logger.error(f"Unexpected error while compressing repository: {e}")

    return None




def compress_all_repos_in_dir(repo_dir: str) -> None:
    """Compress all repositories in the specified directory into zip files."""
    if not os.path.exists(repo_dir):
        logger.error(f"Repository directory {repo_dir} does not exist.")
        return

    n_cores = 8
    logger.info(f"Compressing repositories in {repo_dir} using {n_cores} cores...")
    from tqdm import tqdm
    from multiprocessing import Pool

    dir_paths = [os.path.join(repo_dir, d) for d in os.listdir(repo_dir) if os.path.isdir(os.path.join(repo_dir, d))]
    with Pool(processes=n_cores) as pool:
        # Use tqdm to show progress bar
        for _ in tqdm(pool.imap(compress_repo, dir_paths), total=len(dir_paths), desc="Compressing repositories"):
            pass

def analyse_repo(repo_path) -> Optional[RepoAnalysisResult]:
    name, url = get_repo_name_and_url(repo_path)

    logger.info(f"Analysing {name}")
    logger.info(f"Repository URL: {url}")

    # if not is_repo_active(url):
    #     logger.warning(f"Repository {name} is not active or does not exist.")
    #     return None

    logger.info(f"Repository {name} is active and exists.")
    repo_dir = get_or_clone_repo(url, name)
    if repo_dir is None:
        logger.error(f"Failed to clone or retrieve repository {name}.")
        return None

    results: List[FileAnalysisResult] = extract_sql_from_repo(repo_dir, url)
    queries_count = sum(len(file_result.queries) for file_result in results)
    logger.info(f"Extracted {queries_count} SQL queries from {len(results)} files in from repository {name}.")

    if DELETE_REPOS_AFTER_ANALYSIS:
        delete_repo(name)
    else:
        compress_repo(name)

    return RepoAnalysisResult(
        repo_name=name,
        repo_url=url,
        file_results=results,
    )



if __name__ == "__main__":
    # Example usage
    repos_dir = '/Users/paul/workspace/SqlPile/.data/repos'
    compress_all_repos_in_dir(repos_dir)

