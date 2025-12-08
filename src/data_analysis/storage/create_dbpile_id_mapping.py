from dataclasses import dataclass
from typing import Callable, Tuple

from src.config import HUGGINFACE_DATA_DB_PATH, HUGGINFACE_DATASETS_DB_PATH, get_con, KAGGLE_DATA_DB_PATH, \
    KAGGLE_DATASETS_DB_PATH
from src.data_analysis.storage.huggingface.download_data import get_schema_and_table_name, \
    get_schema_name_from_dataset_id
import duckdb

from src.sql_analysis.utils.names import clean_name


@dataclass
class MappingConfig:
    data_path: str
    dataset_path: str
    datasets_table_name: str
    datasets_id_column_name: str
    download_column_name: str
    id_to_table_name_fn: Callable[[str], str]


def main(config: MappingConfig):
    data_con = duckdb.connect(config.data_path)
    dataset_con = duckdb.connect(config.dataset_path)

    # Create table if not exists
    dataset_con.execute("""
        CREATE OR REPLACE TABLE repo_tables (
            id TEXT,
            schema_name TEXT,
            table_name TEXT
        )
    """)

    dataset_con.execute("""
        CREATE OR REPLACE TABLE dbpile_id_mapping (
            dataset_id TEXT,
            dbpile_repo_id INTEGER
        )
    """)

    # Load dataset IDs
    dataset_ids = [
        id for (id,) in dataset_con
        .execute(f"SELECT {config.datasets_id_column_name} FROM {config.datasets_table_name}").fetchall()
    ]

    # Build mapping: schema → id
    table_to_id = {}
    for dataset_id in dataset_ids:
        schema = config.id_to_table_name_fn(dataset_id)
        table_to_id[schema] = dataset_id

    found_match = 0
    found_no_match = 0

    # Iterate through all tables in the DuckDB repo
    # todo: only make this on table_schema, not table_name
    all_tables = data_con.execute(""" 
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    """).fetchall()

    for schema, table in all_tables:
        key = schema

        if key in table_to_id:
            dataset_id = table_to_id[key]
            # print(f"Table \"{schema}\".\"{table}\" MATCHES dataset id {dataset_id}.")
            found_match += 1

            dataset_con.execute("""
                INSERT INTO repo_tables (id, schema_name, table_name)
                VALUES (?, ?, ?)
            """, (dataset_id, schema, table))

        else:
            # print(f"Table \"{schema}\".\"{table}\" has NO matching dataset entry.")
            found_no_match += 1

    # for all repo_tables, find the id in the dataset_con
    dbpile_con = get_con()

    # create a table in dbpile with meta info on kaggle, huggingface datasets
    dbpile_con.execute("""
        CREATE TABLE IF NOT EXISTS data_source_stats (
            repo_id INTEGER,
            source_id TEXT,
            downloads INTEGER,
            source_type TEXT -- 'huggingface' or 'kaggle'
        )
    """)



    n_successful_mappings = 0
    n_failed_mappings = 0

    for (hf_id, schema_name) in dataset_con.execute("SELECT DISTINCT id, schema_name FROM repo_tables").fetchall():
        # find the repo_id in the main database
        dbpile_id = dbpile_con.execute(f"SELECT id FROM repos WHERE '{schema_name}' in repo_name").fetchone()
        if dbpile_id is None:
            # print(f"Could not find repo_id for dataset id {hf_id} in main database.")
            n_failed_mappings += 1
            continue

        dataset_con.execute("""
            INSERT INTO dbpile_id_mapping (dataset_id, dbpile_repo_id)
            VALUES (?, ?)
        """, (hf_id, dbpile_id[0]))
        n_successful_mappings += 1

        n_downloads = dataset_con.execute(f"""
            SELECT {config.download_column_name} FROM {config.datasets_table_name}
            WHERE {config.datasets_id_column_name} = ?
        """, (hf_id,)).fetchone()

        # Also insert into data_source_stats
        dbpile_con.execute("""
            INSERT INTO data_source_stats (repo_id, source_id, downloads, source_type)
            VALUES (?, ?, ?, ?)
        """, (dbpile_id[0], hf_id, n_downloads[0] if n_downloads else 0,
              'huggingface' if 'huggingface' in config.data_path else 'kaggle'))

    print(f"Total tables with dataset match: {found_match}")
    print(f"Total tables without match: {found_no_match}")
    print(f"Total tables checked: {found_match + found_no_match}")

    print(f"Total successful hf to dbpile id mappings: {n_successful_mappings}")
    print(f"Total failed hf to dbpile id mappings: {n_failed_mappings}")
    print(f"Total hf to dbpile id mappings attempted: {n_successful_mappings + n_failed_mappings}")


if __name__ == "__main__":
    hf_config = MappingConfig(
        data_path=HUGGINFACE_DATA_DB_PATH,
        dataset_path=HUGGINFACE_DATASETS_DB_PATH,
        id_to_table_name_fn=get_schema_name_from_dataset_id,
        datasets_table_name='parse_results',
        datasets_id_column_name='id',
        download_column_name='downloads'
    )

    kaggle_config = MappingConfig(
        data_path=KAGGLE_DATA_DB_PATH,
        dataset_path=KAGGLE_DATASETS_DB_PATH,
        id_to_table_name_fn=clean_name,
        datasets_table_name='kaggle_datasets',
        datasets_id_column_name='ref',
        download_column_name='download_count'
    )

    main(hf_config)
    main(kaggle_config)
