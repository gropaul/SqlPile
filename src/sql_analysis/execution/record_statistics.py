from typing import List

import duckdb
import pandas as pd

from src.config import COLUMNS_TABLE_NAME, TABLES_TABLE_NAME, COLUMN_VALUES_TABLE_NAME, MAX_VALUES_TO_SAVE_PER_COLUMN, \
    MAX_VALUES_TO_ANALYZE_PER_COLUMN, TABLE_VALUES_COUNT_TABLE_NAME, get_con, KAGGLE_DATA_DB_PATH
from src.sql_analysis.execute_queries import quote


def get_string_column_statistics(
        con: duckdb.DuckDBPyConnection,
        sandbox_con: duckdb.DuckDBPyConnection,
        repo_id: int,
        database_schema: str = ''
):

    table_qualifier = f"{quote(database_schema)}." if database_schema else ""

    # first get all the string columns of the repo
    columns_to_analyze = con.execute(f"""
             SELECT {COLUMNS_TABLE_NAME}.id as column_id, column_name, table_name 
             FROM {COLUMNS_TABLE_NAME} 
             JOIN {TABLES_TABLE_NAME} on {COLUMNS_TABLE_NAME}.table_id = {TABLES_TABLE_NAME}.id
             JOIN {TABLE_VALUES_COUNT_TABLE_NAME} on {TABLE_VALUES_COUNT_TABLE_NAME}.table_id = {TABLES_TABLE_NAME}.id
             WHERE 
                column_base_type = 'Text' 
                and {TABLE_VALUES_COUNT_TABLE_NAME}.count > 10
                and {TABLES_TABLE_NAME}.repo_id = {repo_id}
        """).fetchall()
    all_stats = []
    for (column_id, column_name, table_name) in columns_to_analyze:
        stats = sandbox_con.execute(
            f"""
            WITH data AS (
                SELECT
                    {column_id}                              AS column_id,
                    CAST({quote(column_name)} AS VARCHAR)    AS value,
                    LENGTH(CAST({quote(column_name)} AS VARCHAR)) AS value_length
                FROM {table_qualifier}{quote(table_name)}
                LIMIT {MAX_VALUES_TO_ANALYZE_PER_COLUMN}
            ),
            char_counts AS (
                SELECT
                    column_id,
                    value,
                    value_length,
                    
                    -- count characters
                    LENGTH(REGEXP_REPLACE(value, '[^a-z]', '', 'g')) AS lower_alpha_chars,
                    LENGTH(REGEXP_REPLACE(value, '[^A-Z]', '', 'g')) AS upper_alpha_chars,
                    LENGTH(REGEXP_REPLACE(value, '[^0-9]',   '', 'g')) AS digit_chars,
                    LENGTH(REGEXP_REPLACE(value, '[^ ]',   '', 'g')) AS space_chars,
                    (value_length - LENGTH(TRANSLATE(value, '/.?:,-', ''))) AS punct_chars,
                    
                    -- get character percentages
                    lower_alpha_chars / value_length as lower_alpha_chars_perc,
                    upper_alpha_chars / value_length as upper_alpha_chars_perc,
                    space_chars / value_length as space_chars_perc,
                    digit_chars / value_length as digit_chars_perc,
                    punct_chars / value_length as punct_chars_perc,
                    
                FROM data
                WHERE value IS NOT NULL
            )
            SELECT
                column_id,
                (SELECT list(value) FROM (FROM data LIMIT 5))         AS sample_values,
                COUNT(*)                                       AS total_rows,
                COUNT(value)                                   AS non_nulls,
                COUNT(*) - COUNT(value)                        AS nulls,
                COUNT_IF(value = '')                           AS empty_strings,
                COUNT_IF(TRIM(value) = '')                     AS empty_or_whitespace,
                COUNT(DISTINCT value)                          AS distinct_values,

                -- length stats
                AVG(value_length)                              AS avg_length,
                MEDIAN(value_length)                           AS median_length,
                MIN(value_length)                              AS min_length,
                MAX(value_length)                              AS max_length,
                max_length - min_length                        AS range_length,

                -- percentiles (change/extend as needed)
                -- QUANTILE_CONT(value_length, 0.01)              AS p01_length,
                QUANTILE_CONT(value_length, 0.05)              AS p05_length,
                QUANTILE_CONT(value_length, 0.25)              AS p25_length,
                QUANTILE_CONT(value_length, 0.50)              AS p50_length,
                QUANTILE_CONT(value_length, 0.75)              AS p75_length,
                QUANTILE_CONT(value_length, 0.95)              AS p95_length,
                -- QUANTILE_CONT(value_length, 0.99)              AS p99_length
                
                -- character percentages
                AVG(lower_alpha_chars_perc)                    AS avg_lower_alpha_chars,
                AVG(upper_alpha_chars_perc)                    AS avg_upper_alpha_chars,
                AVG(space_chars_perc)                          AS avg_space_chars,
                AVG(digit_chars_perc)                          AS avg_digit_chars,
                AVG(punct_chars_perc)                          AS avg_punct_chars
                
            FROM char_counts
            GROUP BY column_id
            """
        ).fetchdf()


        # get a histogram of the characters in the string values
        # todo: later

        all_stats.append(stats)

    # create one df from the list of dfs
    if all_stats:
        all_stats_df = pd.concat(all_stats, ignore_index=True)
        print(f"Storing {len(all_stats_df)} string column statistics for repo id {repo_id}")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS string_column_statistics AS 
            SELECT * FROM all_stats_df
            LIMIT 0
        """)
        con.execute(f"""
            INSERT INTO string_column_statistics 
            SELECT * FROM all_stats_df
        """)
    else:
        print(f"No string columns found for repo id {repo_id}")





def test_with_kaggle():
    con = get_con()

    sandbox_con = duckdb.connect(KAGGLE_DATA_DB_PATH)

    # get repo information for id 41171
    repo_info = con.execute(f"""
        SELECT id, repo_name, repo_url 
        FROM repos 
        WHERE id = 40988
    """).fetchone()

    id, repo_name, repo_url = repo_info

    # remove the 'kaggle-' prefix from the repo_name to get the schema name
    table_schema = repo_name.replace('3rd-party-kaggle-', '')
    print(f"Processing repo id {id}, name {repo_name}, url {repo_url}, schema {table_schema}")

    # get all tables with this schema
    tables = sandbox_con.execute(f"""
        SELECT * FROM information_schema.tables 
        WHERE table_schema = '{table_schema}'
    """).fetchall()

    # show all available schemas in the kaggle_data database
    all_schemas = sandbox_con.execute(f"""
        SELECT DISTINCT table_schema 
        FROM information_schema.tables 
        ORDER BY table_schema
    """).fetchall()
    print(f"Available schemas in kaggle_data: {[s[0] for s in all_schemas]}")
    print(f"Found {len(tables)} tables in schema {table_schema}")

    get_string_column_statistics(con, sandbox_con, id, table_schema)







