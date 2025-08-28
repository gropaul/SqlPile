
from src.config import get_con
from src.data_analysis.usage_plots import create_stacked_bar_plot


def get_storage_percentage_table():

    con = get_con('/Users/paul/workspace/SqlPile/data/schemapile_tmp.duckdb', read_only=True)

    print \
        ("Todo: The number of columns we have for strings is still lower then the number of other columns as for strings we only take columns that we have values for..")
    query = """
        WITH columns_with_size AS (
            SELECT columns.id as id, tables.id as table_id, columns.column_type, column_base_type, size_in_bytes
            FROM columns
            JOIN sql_type_sizes as sts ON  sts.sql_type = column_type
            JOIN tables ON tables.id = columns.table_id
            JOIN repos ON tables.repo_id = repos.id
            WHERE 
                size_in_bytes is NOT NULL
                AND (
                    -- if the column comes from SqlPile, we only take into account the columns that are used in queries
                    -- as only for these we have information of the size of the text values
                    columns.id IN (SELECT unnest(column_ids) as column_id FROM column_usages)
                    OR get_repo_origin(repo_url) != 'SqlPile'
                )
            UNION ALL
            SELECT id, table_id, column_type, column_base_type, size_in_bytes
            FROM columns
            JOIN string_column_sizes ON column_id = columns.id
        ),
        table_rows AS (
            SELECT table_id, MIN(count) as n_rows
            FROM columns
            LEFT JOIN column_values_count as cvc on columns.id = cvc.column_id
            GROUP BY table_id
            HAVING n_rows is NOT null
            ORDER BY table_id
        ),
        storage_per_repo AS (
            SELECT 
                tables.repo_id,
                column_base_type,
                get_repo_origin(repo_url) AS repo_origin,
                COUNT(*)                        AS cnt,
                SUM(size_in_bytes * n_rows)     AS storage,   -- defer rounding
                SUM(n_rows)                     AS n_values
            FROM columns_with_size 
            JOIN table_rows AS tr USING (table_id)
            JOIN tables ON tables.id = columns_with_size.table_id
            JOIN repos  ON tables.repo_id = repos.id
            WHERE repo_name NOT IN ('3rd-party-sql-storm-tpc-ds', '3rd-party-sql-storm-tpc-h')
            AND 'tpc-h' NOT IN repo_name
            GROUP BY ALL
        ),
        -- list all repos (after filter) with their origin
        repos_base AS (
            SELECT DISTINCT
                r.id AS repo_id,
                get_repo_origin(r.repo_url) AS repo_origin
            FROM repos r
            WHERE r.repo_name NOT IN ('3rd-party-sql-storm-tpc-ds', '3rd-party-sql-storm-tpc-h')
        ),
        -- list all column types observed anywhere
        types AS (
            SELECT DISTINCT column_base_type
            FROM columns_with_size
        ),
        -- complete grid repo x type (so missing types become zeros)
        grid AS (
            SELECT b.repo_id, b.repo_origin, t.column_base_type
            FROM repos_base b
            CROSS JOIN types t
        ),
        storage_filled AS (
          SELECT 
            g.repo_id,
            g.repo_origin,
            g.column_base_type,
            COALESCE(s.cnt, 0)        AS cnt,
            COALESCE(s.storage, 0)    AS storage,
            COALESCE(s.n_values, 0)   AS n_values
          FROM grid g
          LEFT JOIN storage_per_repo s USING (repo_id, column_base_type)
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
            f.column_base_type,
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
            column_base_type,
            ROUND(AVG(column_percentage), 6)  AS column_percentage,
            ROUND(AVG(storage_percentage), 6) AS storage_percentage,
            ROUND(AVG(value_percentage), 6)   AS value_percentage
          FROM percentages
          GROUP BY ALL
        ) FROM aggregates ORDER BY repo_origin, column_base_type

    """
    result = con.execute(query).fetchall()
    # todo

    data = {
        'column_percentage': [],
        'value_percentage': [],
        'storage_percentage': [],
    }

    for repo_origin, column_base_type, column_percentage, storage_percentage, value_percentage in result:
        # row has format ('column_percentage', 'repo_origin', 'column_base_type', percentage)
        data['column_percentage'].append(('column_percentage', repo_origin, column_base_type, column_percentage))
        data['storage_percentage'].append(('storage_percentage', repo_origin, column_base_type, storage_percentage))
        data['value_percentage'].append(('value_percentage', repo_origin, column_base_type, value_percentage))

    plot = create_stacked_bar_plot(data, 'first', '.')



if __name__ == "__main__":
    get_storage_percentage_table()
