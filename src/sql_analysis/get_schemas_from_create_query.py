import re
from typing import Optional

import duckdb
from tqdm import tqdm

from src.config import DATABASE_PATH, get_con
from src.sql_analysis.load_schemapile_json_to_ddb import TABLES_TABLE_NAME, COLUMNS_TABLE_NAME, QUERIES_TABLE_NAME
from src.sql_analysis.tools.semantic_type import get_column_semantic_type
from src.sql_analysis.tools.sql_to_schema import parse_create_table, clean_identifier
from src.sql_analysis.tools.sql_types import unify_type

def get_schemas_from_create_query(repo_id: Optional[int] = None):

    con = get_con()
    con.execute(f"""
        CREATE OR REPLACE TABLE queries_parsing_error (
            repo_id INTEGER,
            query_id INTEGER,
            sql TEXT,
            error_message TEXT
        )
    """)
    max_tables_id = con.execute(f"""
        SELECT MAX(id) FROM {TABLES_TABLE_NAME}
    """).fetchone()[0]
    print(f"Max table id: {max_tables_id}")

    max_columns_id = con.execute(f"""
        SELECT MAX(id) FROM {COLUMNS_TABLE_NAME}
    """).fetchone()[0]

    print(f"Max column id: {max_columns_id}")

    create_queries = con.execute(f"""
    WITH distrinct_queries AS (
        SELECT sql, id, repo_id, type
        FROM {QUERIES_TABLE_NAME}
        WHERE type = 'CREATE' AND not is_create_view_udf(sql)
    )
        SELECT sql, get_table_name_udf(sql) AS query_table_name, queries.id, repos.repo_url, repos.id AS repo_id
        FROM distrinct_queries AS queries
        JOIN repos ON queries.repo_id = repos.id
        WHERE query_table_name IS NOT NULL
          AND ({'repo_id = ' + str(repo_id) if repo_id is not None else 'True'})
          AND query_table_name IS NOT NULL 
          AND NOT EXISTS (
            FROM tables 
            WHERE lower(tables.table_name_clean) = query_table_name and tables.repo_id = repos.id and false
          ) ORDER BY repo_id
    """).fetchall()

    # get the number of queries
    schemas = []
    n_erros = 0
    n_new_tables = 0
    n_new_columns = 0
    for sql, table_name, query_id, repo_url, repo_id in tqdm(create_queries, desc="Processing CREATE TABLE queries",
                                                             unit="query"):
        try:
            table_schema = parse_create_table(sql)

            table_name_clean = clean_identifier(table_schema.table_name)

            # check if the table already exists
            existing_table = con.execute(f"""
                SELECT id FROM {TABLES_TABLE_NAME} 
                WHERE repo_id = ? AND table_name_clean = ?
            """, (repo_id, table_name_clean)).fetchone()

            if existing_table is not None:
                continue

            n_new_tables += 1

            # insert the table schema into the database
            # columns:    id, repo_id, table_name, table_name_clean, file_url
            table_id = max_tables_id + n_new_tables
            con.execute(f"""
                INSERT INTO {TABLES_TABLE_NAME} (id, repo_id, table_name, table_name_clean, file_url)
                VALUES (?, ?, ?, ?, ?)
            """, (table_id, repo_id, table_schema.table_name, table_name_clean, None))

            for column in table_schema.columns:
                # columns: id,table_id,column_name,column_type,column_base_type,column_type_original,semantic_type,is_unique,is_nullable,is_indexed,is_primary_key
                n_new_columns += 1
                column_name = column.name
                column_table_index = column.table_index
                column_type_original = column.type
                column_type, base_type = unify_type(column_type_original)
                semantic_type = get_column_semantic_type(column_name, base_type)

                column_id = max_columns_id + n_new_columns

                con.execute(f"""
                    INSERT INTO {COLUMNS_TABLE_NAME} (
                        id, table_id, column_name, column_table_index, column_type, column_base_type,
                        column_type_original, semantic_type, is_unique, is_nullable,
                        is_indexed, is_primary_key
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    column_id, table_id, column_name, column_table_index, column_type, base_type,
                    column_type_original, semantic_type, column.is_primary_key,
                    True, False, False
                ))

        except Exception as e:
            con.execute(f"""
                INSERT INTO queries_parsing_error (repo_id, query_id, sql, error_message)
                VALUES (?, ?, ?, ?)
            """, (repo_id, query_id, sql, str(e)))
            n_erros += 1

    print(f"Parsed {len(create_queries)} CREATE TABLE queries.")
    print(f"Found {n_new_tables} new tables and {n_new_columns} new columns.")
    print(f"Encountered {n_erros} errors during parsing.")


if __name__ == "__main__":
    get_schemas_from_create_query(repo_id=None)
