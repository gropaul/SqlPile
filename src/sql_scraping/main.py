import json
import os
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb

from analyse_repo import analyse_repo
from src.config import logger, DATA_DIR, DATABASE_PATH
from data_loading import get_urls
from extract_sql import RepoAnalysisResult


con = duckdb.connect(DATABASE_PATH)


def process_url(url: str) -> Optional[int]:
    logger.info(f"Processing URL: {url}")
    result: Optional[RepoAnalysisResult] = analyse_repo(url)

    if result is not None:
        result.save()
        return result.get_number_of_queries()
    return 0


def main():
    total_queries = 0
    n_threads = 10  # Number of threads to use for parallel processing

    urls = get_urls(filter_analysed=True)

    logger.info(f"Total URLs to process: {len(urls)}")
    if not urls:
        logger.error("No URLs found to process. Exiting.")
        return

    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = {executor.submit(process_url, url): url for url in urls}
        for future in as_completed(futures):
            try:
                queries = future.result()
                total_queries += queries if queries is not None else 0
                logger.info(f"Total queries found so far: {total_queries}")
            except Exception as e:
                logger.error(f"Error processing URL {futures[future]}: {e}")


if __name__ == "__main__":
    main()