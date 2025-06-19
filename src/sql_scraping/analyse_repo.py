import os
import shutil
import subprocess
import zipfile
from typing import Tuple, Optional, List

import requests

from src.config import REPO_DIR, logger, PROCESS_ZIPPED_REPOS, REPO_HANDLING
from src.sql_scraping.extract_sql import RepoAnalysisResult, FileAnalysisResult, extract_sql_from_repo, MetaDataFile, \
    META_DATA_FILE_ENDINGS, MAX_METADATA_FILE_SIZE

allowed_providers = ['https://github', 'https://gitlab']

def get_repo_name_and_url(repo_path) -> Tuple[str, str]:
    # make sure the repo_path is a valid GitHub URL
    if not any(provider in repo_path for provider in allowed_providers):
        raise ValueError("Invalid repository URL. Must be a GitHub or GitLab URL: " + repo_path)

    # remove https
    repo_path = repo_path.replace("https://", "")
    # split the URL to get the repo name and URL
    parts = repo_path.split('/')
    username = parts[1]
    repo_name = parts[2]

    if 'github' in repo_path:
        # GitHub URL
        repo_url = f"https://github.com/{username}/{repo_name}"
    elif 'gitlab' in repo_path:
        # GitLab URL
        repo_url = f"https://gitlab.com/{username}/{repo_name}"
    else:
        raise ValueError("Unsupported repository provider. Only GitHub and GitLab are supported.")

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

        # Check for zipped repo
        zip_path = os.path.join(REPO_DIR, f"{repo_name}.zip")
        if os.path.exists(zip_path):
            if PROCESS_ZIPPED_REPOS:
                logger.info(f"Repository {repo_name} found as a zip file at {zip_path}. Unzipping...")
                with zipfile.ZipFile(zip_path, 'r') as zipf:
                    zipf.extractall(REPO_DIR)
                os.remove(zip_path)
                logger.info(f"Unzipped repository {repo_name} successfully. Deleted zip file {zip_path}.")
                return target_path
            else:
                logger.info(f"Repository {repo_name} found as a zip file but will be skipped (PROCESS_ZIPPED_REPOS=False).")
                return None

        # Clone the repository
        logger.info(f"Cloning repository {repo_name} from {repo_url}...")
        subprocess.run(
            ["git", "clone", repo_url, target_path],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        logger.info(f"Repository {repo_name} cloned successfully.")
        return target_path

    except subprocess.CalledProcessError as e:
        if "Authentication failed" in e.stderr or "fatal: could not read" in e.stderr:
            logger.warning(f"Authentication failed or permission denied for {repo_url}. Skipping.")
        else:
            logger.error(f"Error cloning repository: {e.stderr.strip()}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while cloning repository {repo_name}: {e}")
        return None


def delete_repo(repo_name: str) -> None:
    """Delete the cloned repository directory. This is useful for cleanup after analysis."""
    target_path = os.path.join(REPO_DIR, repo_name)
    if os.path.exists(target_path):
        try:
            logger.info(f"Deleting repository {repo_name} at {target_path}...")
            shutil.rmtree(target_path)
            logger.info(f"Repository {repo_name} deleted successfully.")
        except Exception as e:
            logger.error(f"Unexpected error while deleting repository: {e}")
    else:
        logger.warning(f"Repository {repo_name} does not exist at {target_path}. No action taken.")

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

        # Use zipfile module instead of subprocess
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(target_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, os.path.dirname(target_path))
                    zipf.write(file_path, arcname)

        if os.path.exists(zip_path):
            zip_size = os.path.getsize(zip_path)
            logger.info(f"Compressed size of {repo_name}.zip: {zip_size / 1024:.2f} KB")

        logger.info(f"Repository {repo_name} compressed successfully.")

        delete_repo(repo_name)
        return zip_path
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


def get_metadata_from_repo(repo_path: str) -> List[MetaDataFile]:

    # get all metadata files in the repository root directory that have the metadata extensions, only take them
    # if they are below MAX_METADATA_FILE_SIZE

    files_in_repo = os.listdir(repo_path)
    endings = META_DATA_FILE_ENDINGS
    metadata_files = [f for f in files_in_repo if any(f.endswith(ending) for ending in endings)]

    metadata_files = [os.path.join(repo_path, f) for f in metadata_files]
    metadata_files = [f for f in metadata_files if os.path.getsize(f) < MAX_METADATA_FILE_SIZE]
    metadata: List[MetaDataFile] = []
    for metadata_file in metadata_files:
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                content = f.read()

            file_type = os.path.splitext(metadata_file)[1].lower()
            metadata.append(MetaDataFile(file_path=metadata_file, content=content, file_type=file_type))
        except Exception as e:
            logger.error(f"Error reading metadata file {metadata_file}: {e}")

    return metadata



def analyse_repo(repo_url) -> Optional[RepoAnalysisResult]:
    name, url = get_repo_name_and_url(repo_url)

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

    if queries_count > 0:
        meta_data = get_metadata_from_repo(repo_dir)
    else:
        meta_data = []


    if REPO_HANDLING == 'delete_after_processing':
        delete_repo(name)
    elif REPO_HANDLING == 'compress_after_processing':
        zip_path = compress_repo(name)
        if zip_path:
            logger.info(f"Compressed repository {name} into {zip_path}.")
        else:
            logger.error(f"Failed to compress repository {name}.")
    else:
        logger.info(f"Keeping repository {name} at {repo_dir} for further analysis.")


    return RepoAnalysisResult(
        repo_name=name,
        repo_url=url,
        file_results=results,
        metadata_files=meta_data
    )



if __name__ == "__main__":
    # Example usage
    repos_dir = '/Users/paul/workspace/SqlPile/data/repos'
    compress_all_repos_in_dir(repos_dir)
