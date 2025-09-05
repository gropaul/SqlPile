
from src.config import get_con
from src.data_analysis.usage_plots import create_stacked_bar_plot
from src.sql_analysis.tools.get_sql_type_size import create_sql_type_size_table

def create_columns_storage_view(con: duckdb.DuckDBPyConnection):

    # get the string view
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes_text AS ( SELECT 
            column_id,
            offsets: 8,
            all_offsets: count * offsets,
            
            uncompressed: count_non_null * avg_length + all_offsets,
            
            dict_codes: ceil((ceil(log2(count)) / 8)) * count,
            dict_entries: ceil(count_distinct * (avg_length + offsets)), 
            dict_compressed: dict_codes + dict_entries,
            
            fsst: ceil(uncompressed / 3) + all_offsets,
            
            compressed: least(uncompressed, dict_compressed, fsst),
            compression_rate: round(uncompressed / compressed, 2)
        FROM column_stats_text
    ) """)

    # create a view for ints
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes_int AS ( SELECT
            column_id,
            bits_per_value: if(range_value > 0, ceil(log2(range_value)), 0),
            uncompressed: 32 * count,
            compressed: bits_per_value * count
            FROM column_stats_int)
    """)

    # create a view for floats
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes_float AS ( SELECT
            column_id,
            bits_per_value: ceil(32 / 2.5),  -- assuming ALP compression
            uncompressed: 32 * count,
            compressed: bits_per_value * count
            FROM column_stats_float)
    """)

    # create a view for dates
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes_date AS ( SELECT
            column_id,
            bits_per_value: 16,  -- assuming 16 bits per date
            uncompressed: 32 * count,
            compressed: bits_per_value * count
            FROM column_stats_date)
    """)

    # create a view that unifies all the sizes
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes AS (
            SELECT column_id, uncompressed, compressed, 'text' AS type FROM column_sizes_text
            UNION ALL
            SELECT column_id, uncompressed, compressed, 'int' AS type FROM column_sizes_int
            UNION ALL
            SELECT column_id, uncompressed, compressed, 'float' AS type FROM column_sizes_float
            UNION ALL
            SELECT column_id, uncompressed, compressed, 'date' AS type FROM column_sizes_date
        )
    """)
    

def get_storage_percentage_table(group_key: str = 'column_base_type', output_dir: str = '.'):

    con = get_con()

    create_sql_type_size_table(con)

    con.execute("""
        CREATE OR REPLACE VIEW string_column_sizes AS (
            SELECT column_id, AVG(strlen(value))  as size_in_bytes
            FROM column_values
            GROUP BY column_id
        )
    """)
    print("Todo: The number of columns we have for strings is still lower then the number of other columns as for strings we only take columns that we have values for..")
    query = f"""
        WITH semantics AS (
            SELECT *, unify_llm_type(semantic_type) AS semantic_type_llm 
            FROM '/Users/paul/workspace/SqlPile/src/data_analysis/*.csv' -- both kaggle and sql pile
            WHERE semantic_type_llm != 'Test'
        ),
        columns_with_size AS (
            -- these are all the columns with a fixed size
            SELECT columns.id as id, tables.id as table_id, columns.column_type, {group_key}, size_in_bytes
            FROM columns
            LEFT JOIN semantics ON semantics.column_id = columns.id
            JOIN sql_type_sizes as sts ON sts.sql_type = column_type
            JOIN tables ON tables.id = columns.table_id
            JOIN repos ON tables.repo_id = repos.id
            WHERE size_in_bytes is NOT NULL
            AND (
                -- if the column comes from SqlPile, we only take into account the columns that are used in queries
                -- as only for these we have information of the size of the text values
                columns.id IN (SELECT unnest(column_ids) as column_id FROM column_usages)
                OR get_repo_origin(repo_url) != 'SqlPile'
            )
            -- these are all the columns with a variable size (strings)
            UNION ALL
            SELECT columns.id, table_id, column_type, {group_key}, ifnull(size_in_bytes,9.5) -- 9.5 is the average length for sqlstorm
            FROM columns
            LEFT JOIN semantics ON semantics.column_id = columns.id
            LEFT JOIN string_column_sizes ON string_column_sizes.column_id = columns.id
        ),
        table_rows AS (
            SELECT columns.table_id, MIN(count) as n_rows
            FROM columns
            JOIN table_values_count ON table_values_count.table_id = columns.table_id
            GROUP BY columns.table_id
            HAVING n_rows is NOT null
            ORDER BY columns.table_id
        ),
        storage_per_repo AS (
            SELECT 
                tables.repo_id,
                {group_key},
                get_repo_origin(repo_url) AS repo_origin,
                COUNT(*)                        AS cnt,
                SUM(size_in_bytes * n_rows)     AS storage,   -- defer rounding
                SUM(n_rows)                     AS n_values
            FROM columns_with_size 
            JOIN table_rows AS tr USING (table_id)
            JOIN tables ON tables.id = columns_with_size.table_id
            JOIN repos  ON tables.repo_id = repos.id
            GROUP BY ALL
        ),
        -- list all repos (after filter) with their origin
        repos_base AS (
            SELECT DISTINCT
                r.id AS repo_id,
                get_repo_origin(r.repo_url) AS repo_origin
            FROM repos r
        ),
        -- list all column types observed anywhere
        types AS (
            SELECT DISTINCT {group_key}
            FROM columns_with_size
        ),
        -- complete grid repo x type (so missing types become zeros)
        grid AS (
            SELECT b.repo_id, b.repo_origin, t.{group_key}
            FROM repos_base b
            CROSS JOIN types t
        ),
        storage_filled AS (
          SELECT 
            g.repo_id,
            g.repo_origin,
            g.{group_key},
            COALESCE(s.cnt, 0)        AS cnt,
            COALESCE(s.storage, 0)    AS storage,
            COALESCE(s.n_values, 0)   AS n_values
          FROM grid g
          LEFT JOIN storage_per_repo s USING (repo_id, {group_key})
        ),
        repo_sum AS (
            SELECT 
                repo_id,
                repo_origin,
                SUM(cnt)        AS cnt_sum,
                SUM(storage)    AS storage_sum,
                SUM(n_values)   AS n_values_sum
            FROM storage_filled
            GROUP BY ALL
            HAVING cnt_sum > 0 AND storage_sum > 0 AND n_values_sum > 0
        ),
        percentages AS (
          SELECT 
            f.repo_id,
            f.repo_origin,
            f.{group_key},
            (f.cnt::DOUBLE      / NULLIF(r.cnt_sum, 0))      AS column_percentage,
            (f.storage::DOUBLE  / NULLIF(r.storage_sum, 0))  AS storage_percentage,
            (f.n_values::DOUBLE / NULLIF(r.n_values_sum, 0)) AS value_percentage
          FROM storage_filled f
          JOIN repo_sum r USING (repo_id, repo_origin)
          ORDER BY all
        ),
        aggregates AS (
          SELECT 
            repo_origin,
            {group_key},
            ROUND(AVG(column_percentage), 6)  AS column_percentage,
            ROUND(AVG(storage_percentage), 6) AS storage_percentage,
            ROUND(AVG(value_percentage), 6)   AS value_percentage
          FROM percentages
          GROUP BY ALL
        ) FROM aggregates ORDER BY repo_origin, {group_key}

    """
    result = con.execute(query).fetchall()
    # todo



    print('All the column values are only from *used* columns so far')
    key_column_percentage = '\% of Columns'
    key_value_percentage = '\% of Values'
    key_storage_percentage = '\% of Stored Bytes (Uncompressed)'
    data = {
        key_column_percentage: [],
        key_storage_percentage: [],
        key_value_percentage: [],
    }

    for repo_origin, group, column_percentage, storage_percentage, value_percentage in result:
        # row has format ('column_percentage', 'repo_origin', 'column_base_type', percentage)
        data[key_column_percentage].append((key_column_percentage, repo_origin, group, column_percentage))
        data[key_storage_percentage].append((key_storage_percentage, repo_origin, group, storage_percentage))
        data[key_value_percentage].append((key_value_percentage, repo_origin, group, value_percentage))

    plot = create_stacked_bar_plot(data, group_key, output_dir)



if __name__ == "__main__":
    get_storage_percentage_table('column_base_type')
    get_storage_percentage_table('semantic_type_llm')
