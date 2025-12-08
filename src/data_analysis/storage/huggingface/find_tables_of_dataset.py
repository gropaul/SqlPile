from src.config import HUGGINFACE_DATA_DB_PATH, HUGGINFACE_DATASETS_DB_PATH
from src.data_analysis.storage.huggingface.download_data import get_schema_and_table_name
import duckdb

def main():
    data_con = duckdb.connect(HUGGINFACE_DATA_DB_PATH)
    dataset_con = duckdb.connect(HUGGINFACE_DATASETS_DB_PATH, read_only=True)

    found_one = 0
    found_none = 0

    for (id, ) in dataset_con.execute("SELECT id FROM parse_results").fetchall():
        schema_name, table_name = get_schema_and_table_name(id)

        full_table_name = f'"{schema_name}"."{table_name}"'
        result = data_con.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = '{schema_name}' 
              AND table_name = '{table_name}'
        """).fetchone()

        if result[0] > 0:
            print(f"Dataset {id} has table {full_table_name} in the database.")
            found_one += 1
        else:
            print(f"Dataset {id} is MISSING table {full_table_name} in the database.")
            found_none += 1

    print(f"Total datasets with tables found: {found_one}")
    print(f"Total datasets with tables missing: {found_none}")
    print(f"Total datasets checked: {found_one + found_none}, percentage found: {found_one / (found_one + found_none) * 100:.2f}%")




if __name__ == "__main__":
    main()