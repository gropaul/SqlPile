from src.config import HUGGINFACE_DATA_DB_PATH, HUGGINFACE_DATASETS_DB_PATH, get_con
from src.data_analysis.storage.huggingface.download_data import get_schema_and_table_name
import duckdb


def main():
    data_con = duckdb.connect(HUGGINFACE_DATA_DB_PATH)
    dataset_con = duckdb.connect(HUGGINFACE_DATASETS_DB_PATH)

    # Create table if not exists
    dataset_con.execute("""
        CREATE TABLE IF NOT EXISTS repo_tables (
            id TEXT,
            schema_name TEXT,
            table_name TEXT
        )
    """)

    dataset_con.execute("""
        CREATE TABLE IF NOT EXISTS hf_dbpile_id_mapping (
            hf_dataset_id TEXT,
            dbpile_repo_id INTEGER
        )
    """)

    # Clear old entries so the script is idempotent
    dataset_con.execute("DELETE FROM repo_tables")
    dataset_con.execute("DELETE FROM hf_dbpile_id_mapping")

    # Load dataset IDs
    dataset_ids = [
        id for (id,) in dataset_con.execute("SELECT id FROM parse_results").fetchall()
    ]

    # Build mapping: (schema, table) → id
    table_to_id = {}
    for dataset_id in dataset_ids:
        schema, table = get_schema_and_table_name(dataset_id)
        table_to_id[(schema, table)] = dataset_id

    found_match = 0
    found_no_match = 0

    # Iterate through all tables in the DuckDB repo
    all_tables = data_con.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    """).fetchall()

    for schema, table in all_tables:
        key = (schema, table)

        if key in table_to_id:
            dataset_id = table_to_id[key]
            print(f"Table \"{schema}\".\"{table}\" MATCHES dataset id {dataset_id}.")
            found_match += 1

            dataset_con.execute("""
                INSERT INTO repo_tables (id, schema_name, table_name)
                VALUES (?, ?, ?)
            """, (dataset_id, schema, table))

        else:
            print(f"Table \"{schema}\".\"{table}\" has NO matching dataset entry.")
            found_no_match += 1

    # for all repo_tables, find the id in the dataset_con
    dbpile_con = get_con()

    n_successful_mappings = 0
    n_failed_mappings = 0

    for (hf_id, schema_name, table_name) in dataset_con.execute(
            "SELECT id, schema_name, table_name FROM repo_tables").fetchall():
        # find the repo_id in the main database
        dbpile_id = dbpile_con.execute(f"SELECT id FROM repos WHERE '{schema_name}' in repo_name").fetchone()
        if dbpile_id is None:
            print(f"Could not find repo_id for dataset id {hf_id} in main database.")
            n_failed_mappings += 1
            continue

        dataset_con.execute("""
            INSERT INTO hf_dbpile_id_mapping (hf_dataset_id, dbpile_repo_id)
            VALUES (?, ?)
        """, (hf_id, dbpile_id[0]))
        n_successful_mappings += 1


    print(f"Total tables with dataset match: {found_match}")
    print(f"Total tables without match: {found_no_match}")
    print(f"Total tables checked: {found_match + found_no_match}")

    print(f"Total successful hf to dbpile id mappings: {n_successful_mappings}")
    print(f"Total failed hf to dbpile id mappings: {n_failed_mappings}")
    print(f"Total hf to dbpile id mappings attempted: {n_successful_mappings + n_failed_mappings}")


if __name__ == "__main__":
    main()
