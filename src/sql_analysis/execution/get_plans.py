import os.path
import subprocess
import json
from typing import Dict

from src.config import DATABASE_TMP_DIR

BINARY_PATH = "/Users/paul/workspace/duckdb/build/release/duckdb"

def repo_url_to_database_path(gh_url: str) -> str:
    root_dir = DATABASE_TMP_DIR

    hash_url = hash(gh_url)

    return os.path.join(root_dir, f"{hash_url}.duckdb")



def run_query(query: str, database: str = None) -> str:
    # if there are " in the query, replace them with \"
    query = query.replace('"', '\\"')

    if database is None:
        database = ':memory:'

    # run the query using duckdb CLI
    result = subprocess.run(
        [BINARY_PATH, database, "-json", "-c", query],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Query failed: {result.stderr}")

    return result.stdout.strip()

def get_extended_explain(commands_before: str, query: str, database: str = None) -> Dict:
    # Test extended explain
    result = run_query(f"{commands_before}; EXPLAIN (FORMAT JSON) {query};", database)
    # remove trailing comma from the result
    result = result.rstrip(',\n')
    # surround the result with { and }
    result = f"{{{result}}}"
    json_parsed = json.loads(result)

    return json_parsed


if __name__ == "__main__":
    # test_extended_explain('SELECT * FROM test as t1 JOIN test as t2 USING (a1)', '/Users/paul/workspace/duckdb/test_my/test.duckdb')
    get_extended_explain('SELECT * FROM test as t1, test as t2 where t1.a1 = t2.a1', '/Users/paul/workspace/duckdb/test_my/test.duckdb')
    print("All tests passed!")