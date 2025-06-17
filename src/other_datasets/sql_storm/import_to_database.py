from typing import List

from src.other_datasets.sql_storm.config import SQL_STORM_REPO_URL, SQL_STORE_REPO_PATH, WHITELISTED_DIRS
from src.sql_scraping.analyse_repo import get_repo_name_and_url
from src.sql_scraping.extract_sql import extract_sql_from_repo, FileAnalysisResult, RepoAnalysisResult, \
    SqlExtractionParams


files_accepted = 0


def filter_sql_files(file_path: str) -> bool:


    if any(w_dir in file_path for w_dir in WHITELISTED_DIRS):
        if file_path.endswith('.sql') or file_path.endswith('.SQL'):
            global files_accepted
            files_accepted += 1
            return True

    return False


def main():
    name, url = get_repo_name_and_url(SQL_STORM_REPO_URL)
    params: SqlExtractionParams = SqlExtractionParams(
        file_filter=filter_sql_files,
    )
    results: List[FileAnalysisResult] = extract_sql_from_repo(SQL_STORE_REPO_PATH, SQL_STORM_REPO_URL, params)
    print(f"Extracted {len(results)} files from {SQL_STORM_REPO_URL}")
    result = RepoAnalysisResult(
        repo_name=name,
        repo_url=url,
        file_results=results,
    )

    result.save('/Users/paul/workspace/SqlPile/src/other_datasets/sql_storm/out')
    print(f"Files accepted: {files_accepted}")
    print(f"File results: {len(result.file_results)}")
    print(f"N Queries: {result.get_number_of_queries()}")






if __name__ == "__main__":
    main()