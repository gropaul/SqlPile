import os
from typing import List

import duckdb

from src.config import logger, QUERIES_DIR_RAW, SCHEMAPILE_DIR


def get_processed_urls() -> List[str]:

    try:
        result = duckdb.sql(f" SELECT repo_url FROM '{QUERIES_DIR_RAW}/*/*.parquet'").fetchall()

        urls = [row[0] for row in result]
        # make sure th
        logger.info(f"Found {len(urls)} processed URLs in the database.")
        return urls

    except Exception as e:
        logger.error(f"Error fetching URLs from the database: {e}")

    # also check at data/schemapile/existing.parquet
    try:
        result = duckdb.sql(f" SELECT repo_url FROM '{os.path.join(SCHEMAPILE_DIR, 'existing.parquet')}'").fetchall()
        urls = [row[0] for row in result]
        logger.info(f"Found {len(urls)} processed URLs in existing.parquet.")
        return urls
    except Exception as e:
        logger.error(f"Error fetching URLs from existing.parquet: {e}")
    return []


def get_all_urls() -> List[str]:

    parquet_path = os.path.join(SCHEMAPILE_DIR, "repos.parquet")
    result = duckdb.sql(f"SELECT url FROM '{parquet_path}'").fetchall()
    urls = [row[0] for row in result]
    logger.info(f"Found {len(urls)} total URLs in the database.")
    return urls


def get_urls(filter_analysed: bool, shuffle: bool = False) -> List[str]:

    processed_urls = get_processed_urls()
    all_urls = get_all_urls()

    if filter_analysed:
        # create to sets and filter
        processed_set = set(processed_urls)
        all_set = set(all_urls)
        urls = list(all_set - processed_set)

        logger.info(f"Filtered URLs: {len(urls)} remaining after excluding processed URLs.")
    else:
        urls = all_urls
        logger.info(f"Total URLs without filtering: {len(urls)}")

    if not urls:
        logger.warning("No URLs found to process. Please check the database or the filtering criteria.")
        return []

    # Shuffle the URLs if requested
    if shuffle:
        import random
        random.shuffle(urls)
        logger.info("Shuffled the URLs.")

    return urls
