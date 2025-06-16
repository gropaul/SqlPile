import os

import duckdb
from tqdm import tqdm

from src.config import ROOT, DATA_DIR, DATABASE_PATH
from src.sql_analysis.load_schemapile_json_to_ddb import COLUMN_USAGES_TABLE_NAME, primary_key, foreign_key, \
    COLUMNS_TABLE_NAME, QUERIES_TABLE_NAME
from src.sql_analysis.tools.parse_sql import analyse_sql_query


def analyze_queries_in_db():
    # Connect to DuckDB (in-memory database)
    db_path = os.path.join(DATABASE_PATH)
    con = duckdb.connect(db_path, read_only=False)

    # Create the column usages table
    con.execute(f"""
        CREATE OR REPLACE TABLE {COLUMN_USAGES_TABLE_NAME} (
            id BIGINT {primary_key()},
            column_id BIGINT {foreign_key(COLUMNS_TABLE_NAME, 'id')},
            query_id BIGINT {foreign_key(QUERIES_TABLE_NAME, 'id')},
            operator_type VARCHAR,
            expression VARCHAR,
            other_operand VARCHAR
        )
    """)

    result  = con.execute(f"""
        SELECT queries.repo_id, queries.id, ANY_VALUE(queries.sql), 
            list(columns.id ORDER BY columns.id), list(lower(columns.column_name) ORDER BY columns.id), 
            list(tables.id ORDER BY columns.id), list(lower(tables.table_name_clean) ORDER BY columns.id)
        FROM repos
        JOIN queries ON repos.id = queries.repo_id
        JOIN tables as tables on repos.id = tables.repo_id
        JOIN columns ON tables.id = columns.table_id 
        WHERE
            len(tables.table_name_clean) >= 3 
            AND (len(columns.column_name) >= 3 OR lower(columns.column_name) IN ('id'))
            AND queries.type IN ('SELECT')
            AND contains(lower(queries.sql), lower(columns.column_name))
            AND contains(lower(queries.sql), lower(tables.table_name_clean))
        GROUP BY queries.id, queries.repo_id;
    """).fetchall()

    print("Analysis of Queries in Database: Found {} queries to potentially match with columns".format(len(result)))

    n_failed_matches = 0
    n_successful_matches = 0
    for repo_id, query_id, sql, available_column_ids, available_column_names, available_table_ids, available_table_names in tqdm(result, desc="Analyzing Queries"):

        try:
            query_analysis = analyse_sql_query(sql)
        except Exception as e:
            print(f"Error analyzing query {query_id}: {e}")
            continue

        for column_usage in query_analysis.column_usages:
            column_name = column_usage.column_name.lower().strip().split('.')[-1]  # Normalize column name
            # if column name is wrapped with `` remove it
            column_name = column_name.strip('`"[]')
            if not column_name:
                continue
            index = available_column_names.index(column_name) if column_name in available_column_names else -1
            if index == -1:
                print(f"Repo ID: {repo_id}")
                print(f"Query ID: {query_id}")
                print(f"Column '{column_name}'")
                print(f"Query: {sql}")
                print(f"Table IDs: {available_table_ids}")
                print(f"Table Names: {available_table_names}")
                print(f"Available columns: {available_column_names}")
                print("-" * 40)
                n_failed_matches += 1
                continue

            n_successful_matches += 1

            column_id = available_column_ids[index]
            operator_type = column_usage.operator_type
            expression = column_usage.expression.operator if column_usage.expression else None
            other_operand =  column_usage.expression.other_operand if column_usage.expression else None

            con.execute(f"""
                INSERT INTO {COLUMN_USAGES_TABLE_NAME} (id, column_id, query_id, operator_type, expression, other_operand)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (query_id, column_id, query_id, operator_type, expression, other_operand))

    print("Analysis completed and data inserted into the database.")
    count = con.execute(f"SELECT COUNT(*) FROM {COLUMN_USAGES_TABLE_NAME}").fetchone()[0]
    print(f"Total column usages recorded: {count}")

    print(f"Failed matches: {n_failed_matches}, Successful matches: {n_successful_matches}")


if __name__ == "__main__":
    analyze_queries_in_db()