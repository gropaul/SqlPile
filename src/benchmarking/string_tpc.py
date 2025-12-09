import os
import duckdb
import uuid

from datasets import tqdm

from src.config import TPC_DATA_DIR
from src.sql_analysis.add_3rd_party.add_tpc import Benchmark

NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")

def int_to_uuid_string(i: int) -> str:
    return str(uuid.uuid5(NAMESPACE, str(i)))


def transform_tpc_benchmark(benchmark: Benchmark):
    db_path = os.path.join(TPC_DATA_DIR, f'{benchmark}.duckdb')
    db_string_path = os.path.join(TPC_DATA_DIR, f'{benchmark}-string.duckdb')
    db_string_wal_path = f'{db_string_path}.wal'
    # copy the database to a new path
    if os.path.exists(db_string_path):
        os.remove(db_string_path)
    if os.path.exists(db_string_wal_path):
        os.remove(db_string_wal_path)


    os.system(f'cp {db_path} {db_string_path}')

    con = duckdb.connect(db_string_path)
    con.create_function('to_uuid', int_to_uuid_string)

    # get all id columns in the database
    id_columns = con.execute("""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE column_name LIKE '%key'
    """).fetchall()

    for table_name, column_name in tqdm(id_columns, desc="Transforming TPC benchmark columns to UUIDs"):
        # add a new column of type VARCHAR that converts the id to a uuid
        new_column_name = f'{column_name}_uuid'
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN {new_column_name} VARCHAR")
        con.execute(f"UPDATE {table_name} SET {new_column_name} = to_uuid({column_name})")
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name}")
        con.execute(f"ALTER TABLE {table_name} RENAME COLUMN {new_column_name} TO {column_name}")
        print(f"Transformed {table_name}.{column_name} to UUIDs")

if __name__ == "__main__":
    transform_tpc_benchmark('tpc-h')