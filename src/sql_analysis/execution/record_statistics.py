from typing import List

import duckdb
import pandas as pd
from tqdm import tqdm

from src.config import COLUMNS_TABLE_NAME, TABLES_TABLE_NAME, COLUMN_VALUES_TABLE_NAME, MAX_VALUES_TO_SAVE_PER_COLUMN, \
    MAX_VALUES_TO_ANALYZE_PER_COLUMN, TABLE_VALUES_COUNT_TABLE_NAME, get_con, KAGGLE_DATA_DB_PATH
from src.sql_analysis.execute_queries import quote


def get_column_of_type(
        con: duckdb.DuckDBPyConnection,
        repo_id: int,
        column_base_type: str,
        min_values_count: int = 0
) -> List[tuple]:
    """
    Get all columns of a specific base type in a repo that have at least min_values_count values.
    """

    table_name_stats = f"column_stats_{column_base_type.lower()}"

    # check if the stats table exists
    table_exists = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name = '{table_name_stats}'
    """).fetchone()[0] == 1


    if table_exists:
        table_filter = f"AND {COLUMNS_TABLE_NAME}.id NOT IN (SELECT column_id FROM {table_name_stats})"
    else:
        table_filter = ""




    columns = con.execute(f"""
        SELECT {COLUMNS_TABLE_NAME}.id as column_id, column_name, table_name 
        FROM {COLUMNS_TABLE_NAME} 
        JOIN {TABLES_TABLE_NAME} on {COLUMNS_TABLE_NAME}.table_id = {TABLES_TABLE_NAME}.id
        JOIN {TABLE_VALUES_COUNT_TABLE_NAME} on {TABLE_VALUES_COUNT_TABLE_NAME}.table_id = {TABLES_TABLE_NAME}.id
        WHERE 
            column_base_type = '{column_base_type}' 
            and {TABLE_VALUES_COUNT_TABLE_NAME}.count >= {min_values_count}
            and {TABLES_TABLE_NAME}.repo_id = {repo_id}
            {table_filter} 
    """).fetchall()
    return columns

def record_statistics_datetime(
        con: duckdb.DuckDBPyConnection,
        sandbox_con: duckdb.DuckDBPyConnection,
        repo_id: int,
        database_schema: str = ''
):
    # Assuming your type key is 'Timestamp' (per your ValueType). Adjust if needed.
    columns = get_column_of_type(con, repo_id, 'DateTime', min_values_count=0)
    table_qualifier = f"{quote(database_schema)}." if database_schema else ""

    all_stats = []
    for (column_id, column_name, table_name) in columns:
        try:
            stats = sandbox_con.execute(
                f"""
                        WITH vals AS (
                            SELECT
                                {column_id} AS column_id,
                                {quote(column_name)} AS value
                            FROM {table_qualifier}{quote(table_name)}
                            LIMIT {MAX_VALUES_TO_ANALYZE_PER_COLUMN}
                        ),
                        epochs AS (
                            SELECT
                                column_id,
                                value,
                                epoch(value) AS value_epoch
                            FROM vals
                        )
                        SELECT
                            {column_id} AS column_id,
                            (SELECT list(value) FROM (FROM vals LIMIT 5)) AS sample_values,

                            COUNT(*)                        AS count,
                            COUNT(value)                    AS count_non_null,
                            COUNT(*) - COUNT(value)         AS count_null,
                            COUNT(DISTINCT value)           AS count_distinct,
                            count_non_null / NULLIF(count_distinct, 0) AS repeat_rate,

                            MIN(value)                      AS min_value,
                            MAX(value)                      AS max_value,
                            epoch(MAX(value)) - epoch(MIN(value)) AS range_seconds,

                            -- percentiles on epoch seconds, converted back to TIMESTAMP
                            TO_TIMESTAMP(QUANTILE_CONT(value_epoch, 0.05)) AS p05_value,
                            TO_TIMESTAMP(QUANTILE_CONT(value_epoch, 0.25)) AS p25_value,
                            TO_TIMESTAMP(QUANTILE_CONT(value_epoch, 0.50)) AS p50_value,
                            TO_TIMESTAMP(QUANTILE_CONT(value_epoch, 0.75)) AS p75_value,
                            TO_TIMESTAMP(QUANTILE_CONT(value_epoch, 0.95)) AS p95_value,

                            -- date-level distinctness
                            COUNT(DISTINCT CAST(value AS DATE)) AS distinct_dates,

                            -- coarse time-of-day buckets
                            SUM(CASE WHEN EXTRACT(HOUR FROM value) BETWEEN 0  AND 5  THEN 1 ELSE 0 END) AS hours_00_05,
                            SUM(CASE WHEN EXTRACT(HOUR FROM value) BETWEEN 6  AND 11 THEN 1 ELSE 0 END) AS hours_06_11,
                            SUM(CASE WHEN EXTRACT(HOUR FROM value) BETWEEN 12 AND 17 THEN 1 ELSE 0 END) AS hours_12_17,
                            SUM(CASE WHEN EXTRACT(HOUR FROM value) BETWEEN 18 AND 23 THEN 1 ELSE 0 END) AS hours_18_23
                        FROM epochs
                        """
            ).fetchdf()
        except Exception as e:
            print(f"Error analyzing datetime column {column_name} in table {table_name}: {str(e)}")
            print(f"Skipping this column.")
            continue
        all_stats.append(stats)

    if all_stats:
        all_stats_df = pd.concat(all_stats, ignore_index=True)
        print(f"Storing {len(all_stats_df)} datetime column statistics for repo id {repo_id}")

        con.execute("""
            CREATE TABLE IF NOT EXISTS column_stats_datetime AS
            FROM all_stats_df
            LIMIT 0
        """)

        con.execute("""
            INSERT INTO column_stats_datetime
            SELECT * FROM all_stats_df
        """)


def record_statistics_int(
        con: duckdb.DuckDBPyConnection,
        sandbox_con: duckdb.DuckDBPyConnection,
        repo_id: int,
        database_schema: str = ''
):
    # gather integer columns (same helper you use for floats)
    columns = get_column_of_type(con, repo_id, 'Int', min_values_count=0)
    table_qualifier = f"{quote(database_schema)}." if database_schema else ""

    all_stats = []
    for (column_id, column_name, table_name) in columns:
        try:
            stats = sandbox_con.execute(
                f"""
                        WITH vals AS (
                            SELECT
                                {column_id} AS column_id,
                                {quote(column_name)} AS value
                            FROM {table_qualifier}{quote(table_name)}
                            LIMIT {MAX_VALUES_TO_ANALYZE_PER_COLUMN}
                        )
                        SELECT
                            {column_id} AS column_id,
                            (SELECT list(value) FROM (FROM vals LIMIT 5)) AS sample_values,

                            COUNT(*)                    AS count,
                            COUNT(value)                AS count_non_null,
                            COUNT(*) - COUNT(value)     AS count_null,
                            COUNT(DISTINCT value)       AS count_distinct,
                            count_non_null / NULLIF(count_distinct, 0) AS repeat_rate,

                            -- distribution basics
                            MIN(value)                  AS min_value,
                            MAX(value)                  AS max_value,
                            MAX(value) - MIN(value)     AS range_value,
                            AVG(value)                  AS avg_value,      -- returns DOUBLE
                            MEDIAN(value)               AS median_value,

                            -- percentiles (continuous)
                            QUANTILE_CONT(value, 0.05)  AS p05_value,
                            QUANTILE_CONT(value, 0.25)  AS p25_value,
                            QUANTILE_CONT(value, 0.50)  AS p50_value,
                            QUANTILE_CONT(value, 0.75)  AS p75_value,
                            QUANTILE_CONT(value, 0.95)  AS p95_value,

                            -- sign distribution
                            SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END) AS negatives,
                            SUM(CASE WHEN value = 0 THEN 1 ELSE 0 END) AS zeros,
                            SUM(CASE WHEN value > 0 THEN 1 ELSE 0 END) AS positives,

                            -- dispersion
                            STDDEV_SAMP(value)          AS stddev_value,
                            VAR_SAMP(value)             AS variance_value,
                        FROM vals
                        """
            ).fetchdf()
        except Exception as e:
            print(f"Error analyzing int column {column_name} in table {table_name}: {str(e)}")
            print(f"Skipping this column.")
            continue
        all_stats.append(stats)

    if all_stats:
        all_stats_df = pd.concat(all_stats, ignore_index=True)
        print(f"Storing {len(all_stats_df)} integer column statistics for repo id {repo_id}")

        # ensure destination table exists with matching schema
        con.execute("""
            CREATE TABLE IF NOT EXISTS column_stats_int AS 
            FROM all_stats_df
            LIMIT 0
        """)

        con.execute("""
            INSERT INTO column_stats_int
            SELECT * FROM all_stats_df
        """)


def record_statistics_float(
        con: duckdb.DuckDBPyConnection,
        sandbox_con: duckdb.DuckDBPyConnection,
        repo_id: int,
        database_schema: str = ''
):

    columns = get_column_of_type(con, repo_id, 'Float', min_values_count=0)
    table_qualifier = f"{quote(database_schema)}." if database_schema else ""

    all_stats = []
    for (column_id, column_name, table_name) in columns:

        try:
            stats = sandbox_con.execute(
                f"""
                        WITH decimals AS (
                            SELECT 
                            {column_id} AS column_id,
                            {quote(column_name)} AS value,
                            COALESCE(
                                len(REGEXP_REPLACE(TRIM(TRAILING '0' FROM (value::text)), '^[^.]*\.?', '')), 0) 
                                AS digits_after_decimal
                            FROM {table_qualifier}{quote(table_name)}
                            LIMIT {MAX_VALUES_TO_ANALYZE_PER_COLUMN}
                        )
                        SELECT
                            {column_id} AS column_id,
                            (SELECT list(value) FROM (FROM decimals LIMIT 5))         AS sample_values,
                            COUNT(*) AS count,
                            COUNT(value) AS count_non_null,
                            COUNT(*) - COUNT(value) AS count_null,
                            COUNT(DISTINCT value) AS count_distinct,
                            count_non_null / NULLIF(count_distinct, 0) AS repeat_rate,

                            AVG(value) AS avg_value,
                            MEDIAN(value) AS median_value,
                            MIN(value) AS min_value,
                            MAX(value) AS max_value,
                            MAX(value) - MIN(value) AS range_value,

                            -- percentiles (change/extend as needed)
                            QUANTILE_CONT(value, 0.05) AS p05_value,
                            QUANTILE_CONT(value, 0.25) AS p25_value,
                            QUANTILE_CONT(value, 0.50) AS p50_value,
                            QUANTILE_CONT(value, 0.75) AS p75_value,
                            QUANTILE_CONT(value, 0.95) AS p95_value,

                            -- decimal stats
                            AVG(digits_after_decimal) AS avg_digits_after_decimal,     
                            QUANTILE_CONT(digits_after_decimal, 0.05) AS p05_digits_after_decimal,
                            QUANTILE_CONT(digits_after_decimal, 0.25) AS p25_digits_after_decimal,
                            QUANTILE_CONT(digits_after_decimal, 0.50) AS p50_digits_after_decimal,
                            QUANTILE_CONT(digits_after_decimal, 0.75) AS p75_digits_after_decimal,
                            QUANTILE_CONT(digits_after_decimal, 0.95) AS p95_digits_after_decimal,

                            -- sign distribution
                            SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END) AS negatives,
                            SUM(CASE WHEN value = 0 THEN 1 ELSE 0 END) AS zeros,
                            SUM(CASE WHEN value > 0 THEN 1 ELSE 0 END) AS positives,

                            -- std dev and variance
                            STDDEV_SAMP(value) AS stddev_value,
                            VAR_SAMP(value) AS variance_value


                        FROM decimals
                        """
            ).fetchdf()
        except Exception as e:
            print(f"Error analyzing float column {column_name} in table {table_name}: {str(e)}")
            print(f"Skipping this column.")
            continue
        all_stats.append(stats)


    # create one df from the list of dfs
    if all_stats:
        all_stats_df = pd.concat(all_stats, ignore_index=True)
        print(f"Storing {len(all_stats_df)} float column statistics for repo id {repo_id}")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS column_stats_float AS 
            FROM all_stats_df
            LIMIT 0
        """)
        con.execute(f"""
            INSERT INTO column_stats_float 
            SELECT * FROM all_stats_df
        """)





def record_statistics_string(
        con: duckdb.DuckDBPyConnection,
        sandbox_con: duckdb.DuckDBPyConnection,
        repo_id: int,
        database_schema: str = ''
):

    table_qualifier = f"{quote(database_schema)}." if database_schema else ""

    # first get all the string columns of the repo
    columns_to_analyze = get_column_of_type(con, repo_id, 'Text')
    all_stats = []
    for (column_id, column_name, table_name) in columns_to_analyze:
        try:
            stats = sandbox_con.execute(
                f"""
                        WITH data AS (
                            SELECT
                                {column_id}                              AS column_id,
                                CAST({quote(column_name)} AS VARCHAR)    AS value,
                                LENGTH(CAST({quote(column_name)} AS VARCHAR)) AS value_length,
                                split(value, '') as chars
                            FROM {table_qualifier}{quote(table_name)}
                            LIMIT {MAX_VALUES_TO_ANALYZE_PER_COLUMN}
                        ),
                        chars_unnested AS (
                            SELECT unnest(chars) as char 
                            FROM data 
                            -- USING SAMPLE 1_000_000
                        ),
                        char_counts AS (
                            SELECT char, COUNT(*) as cnt
                            FROM chars_unnested
                            GROUP BY char
                        ),
                        stats AS (
                            SELECT
                                column_id,
                                value,
                                value_length,
                                chars,

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
                                punct_chars / value_length as punct_chars_perc

                            FROM data
                            WHERE value IS NOT NULL
                        )
                        SELECT
                            column_id,
                            (SELECT list(value) FROM (FROM data LIMIT 5))         AS sample_values,
                            COUNT(*)                                       AS count,
                            COUNT(value)                                   AS count_non_null,
                            COUNT(*) - COUNT(value)                        AS count_null,
                            COUNT_IF(value = '')                           AS count_empty,
                            COUNT(DISTINCT value)                          AS count_distinct,
                            count_non_null / NULLIF(count_distinct, 0)     AS repeat_rate,


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
                            AVG(punct_chars_perc)                          AS avg_punct_chars,

                            -- get the distinct chars 
                            (SELECT list({{char: char, cnt: cnt}} ORDER BY cnt) FROM char_counts) as char_histogram

                        FROM stats
                        GROUP BY column_id
                        """
            ).fetchdf()

        except Exception as e:
            print(f"Error analyzing string column {column_name} in table {table_name}: {str(e)}")
            print(f"Skipping this column.")
            continue


        # get a histogram of the characters in the string values
        # todo: later

        all_stats.append(stats)

    # create one df from the list of dfs
    if all_stats:
        all_stats_df = pd.concat(all_stats, ignore_index=True)
        print(f"Storing {len(all_stats_df)} string column statistics for repo id {repo_id}")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS column_stats_text AS 
            FROM all_stats_df
            LIMIT 0
        """)
        con.execute(f"""
            INSERT INTO column_stats_text 
            SELECT * FROM all_stats_df
            WHERE column_id NOT IN (SELECT column_id FROM column_stats_text)
        """)
    else:
        print(f"No string columns found for repo id {repo_id}")




def reset_statistics_tables(con: duckdb.DuckDBPyConnection):
    con.execute(f"DROP TABLE IF EXISTS column_stats_text")
    con.execute(f"DROP TABLE IF EXISTS column_stats_int")
    con.execute(f"DROP TABLE IF EXISTS column_stats_float")
    con.execute(f"DROP TABLE IF EXISTS column_stats_datetime")


def record_statistics_for_repo(
        con: duckdb.DuckDBPyConnection,
        sandbox_con: duckdb.DuckDBPyConnection,
        repo_id: int,
        database_schema: str = ''
):
    print(f"Recording statistics for repo id {repo_id} (schema: '{database_schema}')")
    record_statistics_string(con, sandbox_con, repo_id, database_schema)
    record_statistics_int(con, sandbox_con, repo_id, database_schema)
    record_statistics_float(con, sandbox_con, repo_id, database_schema)
    record_statistics_datetime(con, sandbox_con, repo_id, database_schema)

if __name__ == "__main__":

    print('Starting statistics recording for kaggle datasets...')
    con = get_con()

    sandbox_con = duckdb.connect(KAGGLE_DATA_DB_PATH)

    reset_statistics_tables(con)
    print("Reset statistics tables.")

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
        record_statistics_for_repo(con, sandbox_con, id, table_schema)











