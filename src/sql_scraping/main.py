import argparse
import json
import os
import time
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb

from analyse_repo import analyse_repo
from src.config import logger, DATA_DIR, DATABASE_PATH
from data_loading import get_urls
from extract_sql import RepoAnalysisResult

def process_url(url: str) -> Optional[int]:
    logger.info(f"Processing URL: {url}")
    result: Optional[RepoAnalysisResult] = analyse_repo(url)

    if result is not None:
        result.save()
        return result.get_number_of_queries()
    return 0

def get_queries_per_minute(start_time: float, total_queries: int) -> float:
    elapsed_time = time.time() - start_time
    if elapsed_time == 0:
        return 0.0
    return total_queries / (elapsed_time / 60)  # Queries per minute

def get_duration_str(start_time: float) -> str:
    elapsed_time = time.time() - start_time
    if elapsed_time < 60:
        return f"{elapsed_time:.2f} seconds"
    elif elapsed_time < 3600:
        return f"{elapsed_time / 60:.2f} minutes"
    else:
        return f"{elapsed_time / 3600:.2f} hours"

def main():
    parser = argparse.ArgumentParser(description="Run SQL scraping and analysis.")
    parser.add_argument(
        "-t", "--threads",
        type=int,
        default=4,
        help="Number of threads to use for parallel processing (default: 10)"
    )

    args = parser.parse_args()
    n_threads = args.threads

    logger.info(f"Starting SQL scraping and analysis with {n_threads} threads...")
    urls = get_urls(filter_analysed=True, shuffle=True)

    logger.info(f"Total URLs to process: {len(urls)}")
    if not urls:
        logger.error("No URLs found to process. Exiting.")
        return


    total_queries = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = {executor.submit(process_url, url): url for url in urls}
        for future in as_completed(futures):
            try:
                n_queries = future.result()
                total_queries += n_queries if n_queries is not None else 0
                queries_per_minute = get_queries_per_minute(start_time, total_queries)
                queries_per_minute_str = f"{queries_per_minute:.2f}" if queries_per_minute > 0 else "N/A"
                run_duration_str = get_duration_str(start_time)
                logger.info(f"Finished processing URL {futures[future]}: {n_queries} queries found. "
                            f"Total queries: {total_queries}. "
                            f"Queries per minute: {queries_per_minute_str}. "
                            f"Run duration: {run_duration_str}.")
            except Exception as e:
                logger.error(f"Error processing URL {futures[future]}: {e}")


if __name__ == "__main__":
    main()