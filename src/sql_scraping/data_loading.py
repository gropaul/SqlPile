import os
from typing import List, Optional, Tuple

import duckdb

from src.config import logger, QUERIES_DIR_RAW, SCHEMAPILE_DIR, PERMISSIVE_LICENSES


def get_processed_urls() -> List[str]:
    urls = []
    try:
        result = duckdb.sql(f" SELECT repo_url FROM '{QUERIES_DIR_RAW}/*/*.parquet'").fetchall()

        urls = [row[0] for row in result]
        # make sure th
        logger.info(f"Found {len(urls)} processed URLs in the database from raw queries.")

    except Exception as e:
        logger.error(f"Error fetching URLs from the database: {e}")

    # also check at data/schemapile/existing.parquet
    try:
        result = duckdb.sql(f" SELECT repo_url FROM '{os.path.join(SCHEMAPILE_DIR, 'existing.parquet')}'").fetchall()
        urls_existing = [row[0] for row in result]
        logger.info(f"Found {len(urls_existing)} processed URLs in existing.parquet file.")
        urls.extend(urls_existing)
    except Exception as e:
        logger.error(f"Error fetching URLs from existing.parquet: {e}")

    return list(set(urls))


def get_all_urls(permissive_only: bool) -> List[str]:

    where_clause = ""
    if permissive_only:
        licenses_str = ", ".join([f"'{lic}'" for lic in PERMISSIVE_LICENSES])
        where_clause = f"WHERE license IN ({licenses_str})"
        logger.info(f"Filtering URLs to only include permissive licenses: {PERMISSIVE_LICENSES}")

    parquet_path = os.path.join(SCHEMAPILE_DIR, "repos.parquet")
    result = duckdb.sql(f"SELECT url FROM '{parquet_path}' {where_clause}").fetchall()
    urls = [row[0] for row in result]
    logger.info(f"Found {len(urls)} total URLs in the database.")
    return urls

def get_urls(filter_analysed: bool, shuffle: bool = False, permissive_licenses: bool = False, partition: Optional[Tuple[int, int]] = None) -> List[str]:

    processed_urls = get_processed_urls()
    all_urls = get_all_urls(permissive_licenses)

    print(f"Total URLs in database: {len(all_urls)}, Only permissive licenses: {permissive_licenses}")

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

    # sort the URLs for consistency
    urls.sort()
    # Apply partitioning if specified
    if partition is not None:
        part_idx, n_parts = partition
        if part_idx < 0 or part_idx >= n_parts:
            logger.error(f"Invalid partition index {part_idx} for {n_parts} parts.")
            return []
        total_urls = len(urls)
        part_size = total_urls // n_parts
        start_idx = part_idx * part_size
        end_idx = (part_idx + 1) * part_size if part_idx < n_parts - 1 else total_urls
        urls = urls[start_idx:end_idx]
        logger.info(f"Partitioned URLs: Using partition {part_idx + 1}/{n_parts}, URLs from index {start_idx} to {end_idx} (total {len(urls)} URLs).")

    # Shuffle the URLs if requested
    if shuffle:
        import random
        random.shuffle(urls)
        logger.info(f"Shuffled {len(urls)} URLs for processing.")

    return urls
