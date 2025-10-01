from datasets import tqdm

import duckdb
from src.config import get_con, KAGGLE_DATA_DB_PATH
from src.data_analysis.storage.kaggle.kaggle_compress import compress_kaggle
from src.data_analysis.storage.kaggle.kaggle_to_database import delte_kaggle_repos, save_kaggle_in_database
from src.sql_analysis.execution.record_statistics import record_statistics_for_repo


def add_kaggle_statistics():
    print('Starting statistics recording for kaggle datasets...')
    con = get_con()

    sandbox_con = duckdb.connect(KAGGLE_DATA_DB_PATH)


    # get repo information for id 41171
    repo_infos = con.execute(f"""
        SELECT id, repo_name, repo_url 
        FROM repos 
        WHERE repo_name like '3rd-party-kaggle-%'
    """).fetchall()

    print(f"Found {len(repo_infos)} kaggle repos to process.")

    for id, repo_name, repo_url in tqdm(repo_infos):
        # remove the 'kaggle-' prefix from the repo_name to get the schema name
        table_schema = repo_name.replace('3rd-party-kaggle-', '')
        record_statistics_for_repo(con, sandbox_con, id,  table_schema)




def init_kaggle():
    delte_kaggle_repos()
    save_kaggle_in_database()
    add_kaggle_statistics()
    compress_kaggle()


if __name__ == "__main__":
    init_kaggle()