import os

import duckdb
from tqdm import tqdm

from src.config import ROOT, DATA_DIR, DATABASE_PATH
from src.sql_analysis.analyse_sql_query import analyse_sql_query
from src.sql_analysis.load_schema_pile_to_duckdb import COLUMN_USAGES_TABLE_NAME, primary_key, foreign_key, \
    COLUMNS_TABLE_NAME, QUERIES_TABLE_NAME


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
        SELECT queries.id, ANY_VALUE(queries.sql), 
            list(columns.id ORDER BY columns.id), list(lower(columns.column_name) ORDER BY columns.id), 
            list(tables.id ORDER BY columns.id), list(lower(tables.table_name_clean) ORDER BY columns.id)
        FROM repos
        JOIN queries ON repos.id = queries.repo_id
        JOIN tables as tables on repos.id = tables.repo_id
        JOIN columns ON tables.id = columns.table_id 
        WHERE
            contains(lower(queries.sql), lower(columns.column_name)) AND
            contains(lower(queries.sql), lower(tables.table_name_clean))
        GROUP BY queries.id;
    """).fetchall()

    print("Analysis of Queries in Database: Found {} queries".format(len(result)))

    for query_id, sql, column_ids, column_names, table_ids, table_names in tqdm(result, desc="Analyzing Queries"):

        try:
            query_analysis = analyse_sql_query(sql)
        except Exception as e:
            print(f"Error analyzing query {query_id}: {e}")
            continue

        for column, column_usage in query_analysis.column_usages.items():
            column_name = column.lower()
            if not column_name:
                continue
            index = column_names.index(column_name) if column_name in column_names else -1
            if index == -1:
                # print(f"Column '{column_name}'")
                # print(f"Query: {sql}")
                # print(f"Table IDs: {table_ids}")
                # print(f"Table Names: {table_names}")
                # print(f"Available columns: {column_names}")
                # print("-" * 40)
                continue

            column_id = column_ids[index]
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


if __name__ == "__main__":
    analyze_queries_in_db()
