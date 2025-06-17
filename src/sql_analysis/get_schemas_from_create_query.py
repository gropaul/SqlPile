import re
from typing import Optional

import duckdb

from src.config import DATABASE_PATH

from typing import Optional

from src.sql_analysis.load_schemapile_json_to_ddb import TABLE_TABLE_NAME, COLUMNS_TABLE_NAME, QUERIES_TABLE_NAME
from src.sql_analysis.tools.semantic_type import get_column_semantic_type
from src.sql_analysis.tools.sql_to_schema import parse_create_table
from src.sql_analysis.tools.sql_types import unify_type


def udf_get_table_name(query: str) -> Optional[str]:
    """
    Extracts the table name from a CREATE TABLE SQL query.
    Supports:
      - CREATE TABLE
      - CREATE OR REPLACE TABLE
      - CREATE TABLE IF NOT EXISTS
    """
    # Remove extra whitespace and normalize casing for matching
    cleaned_query = ' '.join(query.strip().split())
    pattern = re.compile(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([`"\[\]\w\.]+)',
        re.IGNORECASE
    )

    match = pattern.match(cleaned_query)
    if match:
        name = match.group(1).strip('`"[]')
        # if the table name is a qualified name (e.g., schema.table), return only the table name
        if '.' in name:
            name = name.split('.')[-1]
        return name.lower()
    else:
        return None


def get_schemas_from_create_query():
    con = duckdb.connect(DATABASE_PATH)

    # register the UDF
    con.create_function("get_table_name", udf_get_table_name, null_handling="special")


    max_tables_id = con.execute(f"""
        SELECT MAX(id) FROM {TABLE_TABLE_NAME}
    """).fetchone()[0]
    print(f"Max table id: {max_tables_id}")

    max_columns_id = con.execute(f"""
        SELECT MAX(id) FROM {COLUMNS_TABLE_NAME}
    """).fetchone()[0]

    print(f"Max column id: {max_columns_id}")


    create_queries = con.execute(f"""
        SELECT sql, get_table_name(sql) AS query_table_name, queries.id, repos.repo_url, repos.id AS repo_id, file_path
        FROM {QUERIES_TABLE_NAME} AS queries
        JOIN repos ON queries.repo_id = repos.id
        WHERE get_table_name(queries.sql) IS NOT NULL
          AND queries.type = 'CREATE'
          AND (repos.id = 11781 OR true)
          AND query_table_name IS NOT NULL 
          AND NOT EXISTS (
            SELECT 1
            FROM tables 
            WHERE lower(tables.table_name_clean) = query_table_name and tables.repo_id = repos.id
          );
    """).fetchall()

    # get the number of queries
    schemas = []
    n_erros = 0
    n_new_tables = 0
    n_new_columns = 0
    for sql, table_name, query_id, repo_url, repo_id, file_path in create_queries:
        try:
            table_schema = parse_create_table(sql)

            clean_name = table_schema.table_name.lower()

            # check if the table already exists
            existing_table = con.execute(f"""
                SELECT id FROM {TABLE_TABLE_NAME} 
                WHERE repo_id = ? AND table_name_clean = ?
            """, (repo_id, clean_name)).fetchone()

            if existing_table is not None:
                continue

            n_new_tables += 1

            # insert the table schema into the database
            # columns:    id, repo_id, table_name, table_name_clean, file_url
            table_id = max_tables_id + n_new_tables
            con.execute(f"""
                INSERT INTO {TABLE_TABLE_NAME} (id, repo_id, table_name, table_name_clean, file_url)
                VALUES (?, ?, ?, ?, ?)
            """, (table_id, repo_id, table_schema.table_name, table_schema.table_name.lower(), file_path))

            for column in table_schema.columns:
                # columns: id,table_id,column_name,column_type,column_base_type,column_type_original,semantic_type,is_unique,is_nullable,is_indexed,is_primary_key
                n_new_columns += 1
                column_name = column.name
                column_type_original = column.type
                column_type, base_type = unify_type(column_type_original)
                semantic_type = get_column_semantic_type(column_name, base_type)

                column_id = max_columns_id + n_new_columns

                con.execute(f"""
                    INSERT INTO {COLUMNS_TABLE_NAME} (
                        id, table_id, column_name, column_type, column_base_type,
                        column_type_original, semantic_type, is_unique, is_nullable,
                        is_indexed, is_primary_key
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    column_id, table_id, column_name, column_type, base_type,
                    column_type_original, semantic_type, column.is_primary_key,
                    True, False, False
                ))

        except Exception as e:
            print(sql)
            print(f"Error parsing CREATE TABLE query in repo {repo_url} ({repo_id}): {e}")
            n_erros += 1

    print(f"Parsed {len(create_queries)} CREATE TABLE queries.")
    print(f"Found {n_new_tables} new tables and {n_new_columns} new columns.")
    print(f"Encountered {n_erros} errors during parsing.")



if __name__ == "__main__":
    get_schemas_from_create_query()
