import duckdb
from tqdm import tqdm

from src.config import QUERIES_DIR_PARTITIONED
from src.remote.load_queries_from_cluster import QUERIES_DIAMOND4_PATH, LOCAL_QUERIES_DIR

# print duckdb version
print(f"DuckDB version: {duckdb.__version__}")


def partition_data(chunk_size):
    n_elements = duckdb.sql(f"""
        SELECT COUNT(*) FROM '{QUERIES_DIAMOND4_PATH}'
    """).fetchone()[0]

    print(f"Total number of elements in the file: {n_elements}")
    n_chunks = (n_elements + chunk_size - 1) // chunk_size
    print(f"Number of chunks to be created: {n_chunks}")

    # clear the partitioned directory if it exists
    import os
    if os.path.exists(QUERIES_DIR_PARTITIONED):
        # remove dir and all its contents
        import shutil
        shutil.rmtree(QUERIES_DIR_PARTITIONED)
    os.makedirs(QUERIES_DIR_PARTITIONED, exist_ok=True)

    print(f"Connecting to DuckDB in memory to execute queries...")

    for i in tqdm(range(n_chunks), desc="Processing chunks"):
        con = duckdb.connect(':memory:')
        offset = i * chunk_size

        query = f"""
            COPY (
                SELECT *, hash(repo_url) as url_hash 
                FROM '{QUERIES_DIAMOND4_PATH}' 
                ORDER BY url_hash
                LIMIT {chunk_size} 
                OFFSET {offset} 
            ) TO '{QUERIES_DIR_PARTITIONED}' (FORMAT parquet, PARTITION_BY (url_hash), APPEND)
        """
        con.execute(query)
        con.close()




if __name__ == "__main__":
    chunk_size = 1000
    partition_data(chunk_size)
    print("File splitting completed.")
