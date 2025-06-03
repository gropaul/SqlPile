import os
import subprocess
from typing import Tuple, Optional, List

import requests

from config import REPO_DIR, logger
from extract_sql import extract_sql_from_repo, SqlQuery, RepoAnalysisResult


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

def clone_repo(repo_url: str, repo_name: str) -> Optional[str]:
    """Clone the repository if it doesn't already exist locally."""
    target_path = os.path.join(REPO_DIR, repo_name)
    try:
        if os.path.exists(target_path):
            logger.info(f"Repository {repo_name} already exists at {target_path}.")
            return target_path
        logger.info(f"Cloning repository {repo_name} from {repo_url}...")
        subprocess.run(["git", "clone", repo_url, target_path], check=True)

        logger.info(f"Repository {repo_name} cloned successfully.")
        return target_path
    except subprocess.CalledProcessError as e:
        logger.error(f"Error cloning repository: {e}")
    except Exception as e:
        logger.error(f"Unexpected error while cloning repository: {e}")

    return None


def analyse_repo(repo_path) -> Optional[RepoAnalysisResult]:
    name, url = get_repo_name_and_url(repo_path)

    logger.info(f"Analysing {name}")
    logger.debug(f"Repository URL: {url}")

    if not is_repo_active(url):
        logger.warning(f"Repository {name} is not active or does not exist.")
        return None

    logger.info(f"Repository {name} is active and exists.")
    repo_dir = clone_repo(url, name)
    if repo_dir is None:
        logger.error(f"Failed to clone repository {name}.")
        return None

    logger.info(f"Repository {name} cloned successfully.")

    queries: List[SqlQuery] = extract_sql_from_repo(repo_dir)
    logger.info(f"Extracted {len(queries)} SQL queries from {name}.")

    return RepoAnalysisResult(
        repo_name=name,
        queries=queries,
    )




