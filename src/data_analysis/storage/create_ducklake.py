from src.config import HUGGINFACE_DUCKLAKE_PATH, HUGGINFACE_DATASETS_DB_PATH
import duckdb

def create_hf_ducklake():
    con = duckdb.connect()
    attach_query = f"""
        INSTALL ducklake;
        ATTACH 'ducklake:{HUGGINFACE_DUCKLAKE_PATH}' AS hf_lake;
        USE hf_lake;
    """
    con.execute(attach_query)

     # attach the huggingface datasets database
    con.execute(f"ATTACH '{HUGGINFACE_DATASETS_DB_PATH}' as hf_datasets;")
    files_query = f"""
        SELECT parse_result_id, config_name, split, SUM(size_bytes) as total_bytes, list(path ORDER BY path) AS paths
        FROM hf_datasets.parquet_files 
        GROUP BY ALL 
        HAVING length(paths) > 0
        ORDER BY ALL
     """

    datasets = con.execute(files_query).fetchall()

    for (dataset_id, config_name, split, total_bytes, paths) in datasets:
        fist_path = paths[0]
        table_name = f"{dataset_id}_{config_name}_{split}"

        try:
            # check if the table already exists
            try:
                con.execute(f'SELECT 1 FROM "{table_name}" LIMIT 1;')
                print(f"Table {table_name} already exists, skipping.")
            except duckdb.CatalogException:
                con.execute(f"CREATE TABLE IF NOT EXISTS \"{table_name}\" AS FROM read_parquet('{fist_path}') LIMIT 0;")
                print(f"Creating table {table_name} with data from dataset {dataset_id} with {len(paths)} files.")
                for path in paths:
                    add_file_sql = f"CALL ducklake_add_data_files('hf_lake', '{table_name}', '{path}', allow_missing => True);"
                    con.execute(add_file_sql)
        except Exception as e:
            print(f"Error processing dataset {dataset_id}, config {config_name}, split {split}: {e}")

if __name__ == "__main__":
    create_hf_ducklake()
