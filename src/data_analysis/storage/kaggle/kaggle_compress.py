from external.CompressionBenchmark.tools.benchmark import run_compression_benchmark
from src.config import KAGGLE_DATA_DIR, KAGGLE_DATA_DB_PATH, get_con, COLUMNS_TABLE_NAME, TABLES_TABLE_NAME
from src.sql_analysis.execute_queries import save_columns_compression_results

import os


def compress_kaggle():

    csv_path = f"{KAGGLE_DATA_DIR}/compression_benchmark_results.csv"
    # run_compression_benchmark(KAGGLE_DATA_DB_PATH, csv_path)

    con = get_con()
    query = f"""
        SELECT id as repo_id, repo_name, compression_data.*
        FROM '{csv_path}' as compression_data, repos
        WHERE split("table"[2:], '"."')[1] in repo_name
    """
    print(query)
    df = con.execute(query).df()

    unique_ids = df['repo_id'].unique()
    for repo_id in unique_ids:
        df_repo = df[df['repo_id'] == repo_id]
        tmp_csv_path = f"{KAGGLE_DATA_DIR}/compression_benchmark_results_{repo_id}.csv"
        df_repo.to_csv(tmp_csv_path, index=False)
        repo_name = df_repo['repo_name'].iloc[0]
        repo_schema = '"' + repo_name.replace('3rd-party-kaggle-', '') + '"'
        print(f"Saving compression results for repo_id {repo_id} ({repo_name}) from {tmp_csv_path} with schema {repo_schema}")

        save_columns_compression_results(repo_id, tmp_csv_path, con, schema=repo_schema)



if __name__ == "__main__":
    compress_kaggle()
