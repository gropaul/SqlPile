from typing import List
import duckdb
from src.config import HUGGINFACE_DATASETS_DB_PATH
from src.data_analysis.storage.huggingface.models import ParseResult


def store_results(results: List[ParseResult], db_path: str = HUGGINFACE_DATASETS_DB_PATH, reset: bool = False):
    con = duckdb.connect(db_path)

    if reset:
        con.execute("DROP TABLE IF EXISTS parse_results")
        con.execute("DROP TABLE IF EXISTS configs")
        con.execute("DROP TABLE IF EXISTS data_files")
        con.execute("DROP TABLE IF EXISTS parquet_files")
        con.execute("DROP TABLE IF EXISTS columns")
        con.execute("DROP TABLE IF EXISTS splits")

    con.execute("""
        CREATE TABLE IF NOT EXISTS parse_results (
            id VARCHAR PRIMARY KEY,
            size_categories VARCHAR,
            download_size BIGINT,
            license VARCHAR,
            dataset_size DOUBLE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS configs (
            parse_result_id VARCHAR,
            name VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS data_files (
            parse_result_id VARCHAR,
            config_name VARCHAR,
            split VARCHAR,
            path VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS parquet_files (
            parse_result_id VARCHAR,
            config_name VARCHAR,
            split VARCHAR,
            path VARCHAR,
            size_bytes BIGINT
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS columns (
            parse_result_id VARCHAR,
            name VARCHAR,
            dtype VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS splits (
            parse_result_id VARCHAR,
            name VARCHAR,
            num_bytes DOUBLE,
            num_examples BIGINT
        )
    """)

    for result in results:

        try:
            # check if result already exists
            if con.execute("SELECT COUNT(*) FROM parse_results WHERE id = ?", [result.id]).fetchone()[0] > 0:
                print(f"Result {result.id} already exists, skipping.")
                continue

            con.execute("INSERT INTO parse_results VALUES (?, ?, ?, ?, ?)",
                        [result.id, result.size_categories, result.download_size,
                         result.license, result.dataset_size])

            for config in result.configs:
                con.execute("INSERT INTO configs VALUES (?, ?)", [result.id, config.name])

                for data_file in config.data_files:
                    con.execute("INSERT INTO data_files VALUES (?, ?, ?, ?)",
                                [result.id, config.name, data_file.split, data_file.path])

            for parquet_file in result.parquet_files:
                con.execute("INSERT INTO parquet_files VALUES (?, ?, ?, ?, ?)",
                            [result.id, parquet_file.config, parquet_file.split, parquet_file.path, parquet_file.size_bytes])

            for column in result.columns:
                con.execute("INSERT INTO columns VALUES (?, ?, ?)",
                            [result.id, column.name, column.dtype])

            for split in result.splits:
                con.execute("INSERT INTO splits VALUES (?, ?, ?, ?)",
                            [result.id, split.name, split.num_bytes, split.num_examples])
        except Exception as e:
            print(f"Error storing result {result.id}: {e}")
            continue

    con.close()
