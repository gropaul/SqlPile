
from src.config import get_con
from src.data_analysis.usage_plots import create_stacked_bar_plot
from src.sql_analysis.tools.get_sql_type_size import create_sql_type_size_table
import duckdb
import matplotlib.pyplot as plt


def count_best_algorithm(df):
    # create a table that shows how often an algorithm is best
    algo_best_counts = {
        "Dict": 0,
        "FSST": 0,
        "FSST12": 0,
        "OnPair16": 0,
        "OnPair": 0,
        "OnPairMini": 0
    }

    for _, row in df.iterrows():
        ratios = {
            "Dict": row["dict_ratio"],
            "FSST": row["fsst_ratio"],
            "FSST12": row["fsst12_ratio"],
            "OnPair16": row["onpair16_ratio"],
            "OnPair": row["onpair_ratio"],
            "OnPairMini": row["onpairmini_ratio"]
        }
        best_algo = max(ratios, key=ratios.get)
        algo_best_counts[best_algo] += 1

    return algo_best_counts


def create_columns_storage_view(con: duckdb.DuckDBPyConnection):
    create_sql_type_size_table(con)

    # get the string view
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes_text AS ( 
            WITH results AS ( 
                SELECT column_id,( compressed_size / n_rows_not_empty) as bytes_per_row_compressed , (uncompressed_size / n_rows_not_empty) as bytes_per_row_uncompressed, algorithm,  FROM columns_compression_results
                WHERE row_group_idx = 0
            ), 
            compression_pivot AS (
                PIVOT results 
                ON algorithm IN ('FSST', 'FSST12', 'OnPair16', 'OnPairMini', 'OnPair', 'Dictionary', 'LZ4')
                USING MIN(bytes_per_row_compressed)
                ORDER BY column_id
            )
            SELECT 
                column_id,
                total_table_size: total_table_size,
                count: count,
                count_non_null: count_non_null,
                offset_size_bytes: 8,
                offset_size_bits_bitpacked: log2(count),
                
                all_offsets_bytes: count * offset_size_bytes,
                all_offsets_bytes_bitpacked: ceil((count * offset_size_bits_bitpacked)/8),
                
                uncompressed: bytes_per_row_uncompressed * count + all_offsets_bytes,
                
                dictionary_compressed: dictionary * count,
                fsst_compressed: fsst * count + all_offsets_bytes_bitpacked,
                fsst12_compressed: fsst12 * count  + all_offsets_bytes_bitpacked,
                onpair16_compressed: onpair16  * count  + all_offsets_bytes_bitpacked,
                onpair_compressed: onpair  * count  + all_offsets_bytes_bitpacked,
                onpairmini_compressed: onpairmini  * count  + all_offsets_bytes_bitpacked,
                lz4_compressed: lz4 * count + all_offsets_bytes,
                compressed: least(fsst_compressed, fsst12_compressed, onpair16_compressed, lz4_compressed, dictionary_compressed),
                compression_rate: round(uncompressed / compressed, 2)
            FROM column_stats_text
            JOIN compression_pivot USING (column_id)
            ORDER BY compression_rate DESC
        ) 
        """)

    # create a view for ints
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes_int AS ( SELECT
            column_id,
            total_table_size: total_table_size,
            size_in_bytes: size_in_bytes,
            count: count,
            count_non_null: count_non_null,
            count_distinct: count_distinct,
            
            bytes_per_value_uncompressed: ifnull(size_in_bytes, 8),
            uncompressed: bytes_per_value_uncompressed * count_non_null,
            
            range_value: range_value,
            bits_per_value_packed: if(range_value > 1, ceil(log2(range_value)), 64),
            bitpacked: ceil((bits_per_value_packed * count_non_null) / 8),
            
            dict_bits_per_code: if(count_distinct > 0, ceil(log2(count_distinct)), 0),
            dict_codes_bytes_size: ceil((dict_bits_per_code * count) / 8),
            dict_size: size_in_bytes * count_distinct,
            dict_compressed: dict_codes_bytes_size + dict_size,
            
            const: if(count_distinct <= 1, 8, NULL),
            
            compressed: least(bitpacked, dict_compressed, const, uncompressed),
            
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
            count_non_null: count_non_null,
            
            bits_per_value_alp: ceil(32 / 3),  -- assuming ALP compression
            total_bytes_alp: ceil((bits_per_value_alp * count_non_null) / 8),
            
            dict_bits_per_code: if(count_distinct > 0, ceil(log2(count_distinct)), 0),
            dict_codes_bytes_size: ceil((dict_bits_per_code * count) / 8),
            dict_size: 4 * count_distinct,
            dict_compressed: dict_codes_bytes_size + dict_size,
            
            uncompressed: 4 * count_non_null,
            compressed: least(total_bytes_alp, dict_compressed, uncompressed)
            FROM column_stats_float)
    """)


    # create a view for dates
    # create table column_stats_datetime if not exists
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes_date AS ( SELECT
            column_id,
            total_table_size: total_table_size,
            count: count,
            count_non_null: count_non_null,
            bits_per_value: 2,  -- assuming 16 bits per date
            uncompressed: 4 * count_non_null,
            compressed: bits_per_value * count_non_null
            FROM column_stats_datetime)
    """)

    # print("ADD DATE SIZE ESTIMATION")

    # create a view that unifies all the sizes
    con.execute("""
        CREATE OR REPLACE VIEW column_sizes AS (
            with data AS (
                SELECT column_id, uncompressed, compressed, count, count_non_null, total_table_size, 'text' AS type FROM column_sizes_text
                UNION ALL
                SELECT column_id, uncompressed, compressed, count, count_non_null, total_table_size, 'int' AS type FROM column_sizes_int
                UNION ALL
                SELECT column_id, uncompressed, compressed, count, count_non_null, total_table_size, 'float' AS type FROM column_sizes_float
                UNION ALL
                SELECT column_id, uncompressed, compressed, count, count_non_null, total_table_size, 'date' AS type FROM column_sizes_date
            ) 
                SELECT 
                    total_table_size / count AS size_factor,
                    column_id, 
                    uncompressed * size_factor AS uncompressed, 
                    compressed * size_factor AS compressed,
                    total_table_size, count, type
                FROM data
                WHERE 
                    True
                    AND count_non_null  > 122_880 / 2 
                    AND compressed > 0
                    AND uncompressed > 0
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
    query = f"""
        WITH columns_with_size AS (
            -- these are all the columns with a fixed size
            SELECT 
                columns.id as id, tables.id as table_id, columns.column_type, {group_key}, 
                uncompressed, compressed, table_values_count.count AS n_rows
            FROM columns
            JOIN tables ON tables.id = columns.table_id
            JOIN table_values_count ON table_values_count.table_id = columns.table_id
            LEFT JOIN column_sizes ON column_sizes.column_id = columns.id
        ),
        storage_per_repo AS (
            SELECT 
                tables.repo_id,
                {group_key},
                get_repo_group(repo_url)       AS repo_origin,
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
          WHERE repo_origin NOT in ('DBPile', 'Other')
          GROUP BY ALL
        ) FROM aggregates ORDER BY repo_origin, {group_key}

    """

    result = con.execute(query).fetchall()
    # also save as csv
    df = con.execute(query).df()

    df.to_csv(f"{output_dir}/storage_percentage_by_{group_key}.csv", index=False)
    # todo



    print('All the column values are only from *used* columns so far')
    key_column_percentage = '\% of Columns'
    key_value_percentage = '\% of Values'
    key_uncompressed = '\% of Uncompressed Bytes'
    key_compressed = '\% of Compressed Bytes'
    data = {
        key_column_percentage: [],
        key_uncompressed: [],
        key_value_percentage: [],
        key_compressed: []
    }

    all_usage_times = [
        key_column_percentage,
        key_value_percentage,
        key_uncompressed,
        key_compressed
    ]

    for repo_origin, group, column_percentage, value_percentage, compressed_percentage, uncompressed_percentage in result:
        data[key_column_percentage].append((key_column_percentage, repo_origin, group, column_percentage))
        data[key_value_percentage].append((key_value_percentage, repo_origin, group, value_percentage))
        data[key_compressed].append((key_uncompressed, repo_origin, group, compressed_percentage))
        data[key_uncompressed].append((key_compressed, repo_origin, group, uncompressed_percentage))

    plot = create_stacked_bar_plot(data, group_key, output_dir, all_usage_times)



if __name__ == "__main__":
    get_storage_percentage_table('column_base_type')
    get_storage_percentage_table('semantic_type_llm')
