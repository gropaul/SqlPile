from src.config import HUGGINFACE_DATA_DB_PATH, HUGGINFACE_DATASETS_DB_PATH
from src.data_analysis.storage.huggingface.download_data import get_schema_and_table_name
import duckdb

def main():
    data_con = duckdb.connect(HUGGINFACE_DATA_DB_PATH)
    dataset_con = duckdb.connect(HUGGINFACE_DATASETS_DB_PATH, read_only=True)

    # Get all dataset IDs that *should* exist
    dataset_ids = {
        id for (id,) in dataset_con.execute("SELECT id FROM parse_results").fetchall()
    }

    # Reverse mapping: build schema/table pairs we *expect*
    # to find from each dataset ID.
    expected_tables = set()
    for id in dataset_ids:
        schema, table = get_schema_and_table_name(id)
        expected_tables.add((schema, table))

    found_match = 0
    found_no_match = 0

    # Iterate over all actual tables present in the database
    all_tables = data_con.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    """).fetchall()

    for schema, table in all_tables:
        if (schema, table) in expected_tables:
            print(f"Table \"{schema}\".\"{table}\" MATCHES a dataset.")
            found_match += 1
        else:
            print(f"Table \"{schema}\".\"{table}\" has NO matching dataset entry.")
            found_no_match += 1

    print(f"Total tables with dataset match: {found_match}")
    print(f"Total tables without match: {found_no_match}")
    print(f"Total tables checked: {found_match + found_no_match}")

if __name__ == "__main__":
    main()
