
from src.config import get_con
from src.data_analysis.usage_plots import create_stacked_bar_plot
from src.sql_analysis.tools.get_sql_type_size import create_sql_type_size_table
import duckdb

def create_columns_storage_view(con: duckdb.DuckDBPyConnection):
    create_sql_type_size_table(con)

    # get the string view
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes_text AS ( SELECT 
            column_id,
            total_table_size: total_table_size,
            count: count,
            offset_size_bytes: 8,
            offset_size_bits_bitpacked: log2(count),
            
            all_offsets_bytes: count * offset_size_bytes,
            all_offsets_bytes_bitpacked: ceil((count * offset_size_bits_bitpacked)/8),
            
            uncompressed: count_non_null * avg_bytes + all_offsets_bytes,
            
            dict_code_size_bitpacked_bits: greatest(log2(count_distinct), 1),
            dict_codes_bytes: ceil((dict_code_size_bitpacked_bits * count) / 8),
            dict_offset_size_bits_bitpacked: log2(count_distinct),
            all_dict_offset_size_bytes_bitpacked: ceil((count_distinct * dict_offset_size_bits_bitpacked) / 8),
            dict_entries_bytes: ceil(total_distinct_bytes + all_dict_offset_size_bytes_bitpacked), 
            dict_compressed: dict_codes_bytes + dict_entries_bytes,
            
            fsst: ceil(uncompressed / 2) + all_offsets_bytes_bitpacked,
            
            compressed: least(uncompressed, dict_compressed, fsst),
            compression_rate: round(uncompressed / compressed, 2)
        FROM column_stats_text
        WHERE count_distinct > 0
        ) 
        """)



    # create a view for ints
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes_int AS ( SELECT
            column_id,
            total_table_size: total_table_size,
            size_in_bytes: size_in_bytes,
            count: count,
            bits_per_value_uncompressed: if(range_value > 0, ceil(log2(range_value)), 0),
            bytes_per_value_compressed: ifnull(size_in_bytes, 8),
            uncompressed: bytes_per_value_compressed * count,
            compressed: ceil((bits_per_value_uncompressed * count) / 8)
            FROM column_stats_int
            JOIN columns ON column_stats_int.column_id = columns.id
            LEFT JOIN sql_type_sizes ON columns.column_type = sql_type_sizes.sql_type
            )
    """)

    # create a view for floats
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes_float AS ( SELECT
            column_id,
            total_table_size: total_table_size,
            count: count,
            bits_per_value: ceil(32 / 3),  -- assuming ALP compression
            uncompressed: 4 * count,
            compressed: ceil((bits_per_value * count) / 8)
            FROM column_stats_float)
    """)


    # create a view for dates
    # create table column_stats_datetime if not exists
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes_date AS ( SELECT
            column_id,
            total_table_size: total_table_size,
            count: count,
            bits_per_value: 2,  -- assuming 16 bits per date
            uncompressed: 4 * count,
            compressed: bits_per_value * count
            FROM column_stats_datetime)
    """)

    # print("ADD DATE SIZE ESTIMATION")

    # create a view that unifies all the sizes
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes AS (
            with data AS (
                SELECT column_id, uncompressed, compressed, count, total_table_size, 'text' AS type FROM column_sizes_text
                UNION ALL
                SELECT column_id, uncompressed, compressed, count, total_table_size, 'int' AS type FROM column_sizes_int
                UNION ALL
                SELECT column_id, uncompressed, compressed, count, total_table_size, 'float' AS type FROM column_sizes_float
                UNION ALL
                SELECT column_id, uncompressed, compressed, count, total_table_size, 'date' AS type FROM column_sizes_date
            ) 
                SELECT 
                    total_table_size / count AS size_factor,
                    column_id, 
                    uncompressed * size_factor AS uncompressed, 
                    compressed * size_factor AS compressed,
                    total_table_size, count, type
                FROM data
                WHERE count > 0
        )
    """)

    # per type, return the sum of uncompressed and compressed
    df = con.execute("SELECT type, SUM(uncompressed) AS uncompressed, SUM(compressed) AS compressed, SUM(count) as n_rows, count(*) as n_columns FROM column_sizes GROUP BY type").fetchall()
    print("Column sizes per type:")
    # print table header
    print(f"{'Type':<10} {'Uncompressed (MB)':<20} {'Compressed (MB)':<20} {'#Rows':<10} {'#Columns':<10} {'Compression Ratio':<20}")
    for type, uncompressed, compressed, rows, cnt in df:
        compression_ratio = round(uncompressed / compressed, 2) if compressed > 0 else None
        print(f"{type:<10} {uncompressed / (1024 * 1024):<20.2f} {compressed / (1024 * 1024):<20.2f} {rows:<10} {cnt:<10} {compression_ratio:<20}")


    

def get_storage_percentage_table(group_key: str = 'column_base_type', output_dir: str = '.'):

    con = get_con()


    create_columns_storage_view(con)

    df_view = con.execute("SELECT * FROM column_sizes").df()
    print(f"Column sizes view has {len(df_view)} rows")

    # con.execute("CALL start_ui()")
    print("Todo: The number of columns we have for strings is still lower then the number of other columns as for strings we only take columns that we have values for..")
    query = f"""
        WITH semantics AS (
            SELECT *, unify_llm_type(semantic_type) AS semantic_type_llm 
            FROM '/Users/paul/workspace/SqlPile/src/data_analysis/*.csv' -- both kaggle and sql pile
            WHERE semantic_type_llm != 'Test'
        ),
        columns_with_size AS (
            -- these are all the columns with a fixed size
            SELECT 
                columns.id as id, tables.id as table_id, columns.column_type, {group_key}, 
                uncompressed, compressed, table_values_count.count AS n_rows
            FROM columns
            LEFT JOIN semantics ON semantics.column_id = columns.id
            JOIN tables ON tables.id = columns.table_id
            JOIN column_sizes ON column_sizes.column_id = columns.id
            JOIN table_values_count ON table_values_count.table_id = columns.table_id
            WHERE n_rows > 70_000
        ),
        storage_per_repo AS (
            SELECT 
                tables.repo_id,
                {group_key},
                get_repo_origin(repo_url)       AS repo_origin,
                COUNT(*)                        AS cnt,
                SUM(uncompressed)               AS uncompressed,
                SUM(compressed)                 AS compressed,
                SUM(n_rows)                     AS n_values
            FROM columns_with_size 
            JOIN tables ON tables.id = columns_with_size.table_id
            JOIN repos  ON tables.repo_id = repos.id
            GROUP BY ALL
        ),
        -- list all repos (after filter) with their origin
        repos_base AS (
            SELECT DISTINCT repo_id, repo_origin
            FROM storage_per_repo r
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
            COALESCE(s.uncompressed, 0) AS uncompressed,
            COALESCE(s.compressed, 0)   AS compressed,
            COALESCE(s.n_values, 0)   AS n_values
          FROM grid g
          LEFT JOIN storage_per_repo s USING (repo_id, {group_key})
        ),
        repo_sum AS (
            SELECT 
                repo_id,
                repo_origin,
                SUM(cnt)        AS cnt_sum,
                SUM(uncompressed)               AS uncompressed_sum,
                SUM(compressed)                 AS compressed_sum,
                SUM(n_values)   AS n_values_sum
            FROM storage_filled
            GROUP BY ALL
        ),
        percentages AS (
          SELECT 
            f.repo_id,
            f.repo_origin,
            f.{group_key},
            (f.cnt::DOUBLE      / NULLIF(r.cnt_sum, 0))      AS column_percentage,
            (f.n_values::DOUBLE / NULLIF(r.n_values_sum, 0)) AS value_percentage,
            (f.compressed::DOUBLE / NULLIF(r.compressed_sum, 0)) AS compressed_percentage,
            (f.uncompressed::DOUBLE / NULLIF(r.uncompressed_sum, 0)) AS uncompressed_percentage,
            
          FROM storage_filled f
          JOIN repo_sum r USING (repo_id, repo_origin)
          ORDER BY all
        ),
        aggregates AS (
          SELECT 
            repo_origin,
            {group_key},
            ROUND(AVG(column_percentage), 6)  AS column_percentage,
            ROUND(AVG(value_percentage), 6)   AS value_percentage,
            ROUND(AVG(compressed_percentage), 6) AS compressed_percentage,
            ROUND(AVG(uncompressed_percentage), 6) AS uncompressed_percentage,
          FROM percentages
          GROUP BY ALL
        ) FROM aggregates ORDER BY repo_origin, {group_key}

    """
    result = con.execute(query).fetchall()
    # also save as csv
    df = con.execute(query).df()
    print(df)

    df.to_csv(f"{output_dir}/storage_percentage_by_{group_key}.csv", index=False)
    # todo



    print('All the column values are only from *used* columns so far')
    key_column_percentage = '\% of Columns'
    key_value_percentage = '\% of Values'
    key_uncompressed = '\% of Stored Bytes (Uncompressed)'
    key_compressed = '\% of Stored Bytes (Compressed)'
    data = {
        key_column_percentage: [],
        key_uncompressed: [],
        key_value_percentage: [],
        key_compressed: []
    }

    for repo_origin, group, column_percentage, value_percentage, compressed_percentage, uncompressed_percentage in result:
        data[key_column_percentage].append((key_column_percentage, repo_origin, group, column_percentage))
        data[key_value_percentage].append((key_value_percentage, repo_origin, group, value_percentage))
        data[key_compressed].append((key_uncompressed, repo_origin, group, compressed_percentage))
        data[key_uncompressed].append((key_compressed, repo_origin, group, uncompressed_percentage))


    plot = create_stacked_bar_plot(data, group_key, output_dir)



if __name__ == "__main__":
    get_storage_percentage_table('column_base_type')
    get_storage_percentage_table('semantic_type_llm')
