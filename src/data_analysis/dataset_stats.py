import os
from typing import Literal

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
# import seaborn as sns

from src.config import PLOTS_DIR, DATABASE_PATH, get_con

OutputFormat = Literal['markdown', 'latex']


def format_df(
        df: pd.DataFrame,
        output_format: OutputFormat = 'markdown',
        label: str = '',
        caption: str = ''
) -> str:
    """
    Format a DataFrame to a string in the specified output format.
    """
    if output_format == 'markdown':
        return df.to_markdown(index=False, tablefmt="pipe")
    elif output_format == 'latex':
        df = df.rename(columns={c: f"\\textbf{{{c}}}" for c in df.columns})

        return df.to_latex(index=False,
                           caption=caption,

                           label=label,
                           float_format="%.3f",
                           column_format='l' + 'c' * (len(df.columns) - 1),
                           escape=False)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")


def get_query_type_query(con: duckdb.DuckDBPyConnection, output_format: OutputFormat = 'markdown') -> str:
    """
    Create a view with the number of operators in a query
    """

    # get the number of queries per type
    df = con.execute("""
                     SELECT type as "Type", COUNT(*) AS "Count"
                     FROM queries
                     GROUP BY type
                     ORDER BY 2 DESC;
                     """).df()

    # add a row with the total number of queries
    total_queries = df['Count'].sum()
    df = df._append({'Type': 'Total', 'Count': total_queries}, ignore_index=True)

    # create a nice md table
    df = df.round(2)
    table_query_types = format_df(df, output_format)

    return table_query_types


def get_operator_table(con: duckdb.DuckDBPyConnection, output_format: OutputFormat = 'markdown',
                       label: str = 'tab-number-of-operators',
                       caption: str = 'Distribution of Operator Types in Queries') -> str:
    query = """
            WITH node_counts_per_query AS (SELECT query_id,
                                                  node_id,
                                                  get_repo_origin(repos.repo_url) as repo_origin,
                                                  MIN(usage_type_to_operator(usage_type))  AS op
                                           FROM column_usages
                                                    JOIN queries ON query_id = queries.id
                                                    JOIN repos ON queries.repo_id = repos.id
                                           GROUP BY query_id, node_id, repo_origin),
                 node_counts AS (SELECT query_id, op, repo_origin, COUNT(*) as op_count
                                 FROM node_counts_per_query
                                 GROUP BY query_id, op, repo_origin),
                 queries_count AS (SELECT repo_origin, COUNT(DISTINCT query_id) as query_count
                                   FROM node_counts
                                   GROUP BY repo_origin),
                 aggregates AS (SELECT oc.op,
                                       oc.repo_origin,
                                       CAST(SUM(oc.op_count) as INT)     as op_count,
                                       COUNT(DISTINCT oc.query_id)       as query_count,
                                       SUM(oc.op_count) / qc.query_count as op_per_query
                                FROM node_counts oc
                                         JOIN queries_count qc ON oc.repo_origin = qc.repo_origin
                                GROUP BY oc.op, qc.query_count, oc.repo_origin
                                ORDER BY 3 DESC),
                 pivoted AS (
                PIVOT aggregates
            ON repo_origin
                USING MIN(op_count) as op_count, MIN(query_count) as query_count, MIN(op_per_query) as op_per_query
            ORDER BY SqlPile_op_per_query
            DESC)
            SELECT op                 AS "Operator",
                   SqlPile_op_per_query AS "SqlPile",
                    SQLStorm_op_per_query AS "SQLStorm",
                   TPC_op_per_query  AS "TPC-[H, DS]"
            FROM pivoted;
            """

    df = con.execute(query).df()

    # create a nice md table
    df = df.round(4)

    table_operators = format_df(df, output_format, label, caption)
    return table_operators


def test_get_operator_table():
    con = get_con(read_only=True)
    print(get_operator_table(con, 'latex'))  # type: ignore





def get_column_type_table(con: duckdb.DuckDBPyConnection, output_format: OutputFormat = 'markdown',
                          label: str = 'tab-number-of-operators',
                          caption: str = 'Distribution of Operator Types in Queries') -> str:
    query = """
            WITH column_counts AS (SELECT column_base_type,
                                          get_repo_group(repos.repo_url) AS repo_origin,
                                          COUNT(*)                       AS column_count
                                   FROM columns
                                            JOIN TABLES ON columns.table_id = tables.id
                                            JOIN REPOS ON tables.repo_id = repos.id
                                   WHERE repo_origin != 'Other'
                                   GROUP BY column_base_type, repo_origin),
                 columns_total_count AS (SELECT repo_origin, SUM(column_count) as total_count
                                         FROM column_counts
                                         GROUP BY repo_origin),
                 counts_aggregated AS (SELECT cc.column_base_type,
                                              cc.repo_origin,
                                              cc.column_count                   AS column_count,
                                              cc.column_count / ctc.total_count AS column_perc
                                       FROM column_counts cc
                                                JOIN columns_total_count ctc ON cc.repo_origin = ctc.repo_origin
                                       WHERE column_perc > 0.01
                                       ORDER BY cc.column_base_type, cc.repo_origin),
                 pivoted AS (
                PIVOT counts_aggregated
            ON repo_origin
                USING MIN(column_count) as column_count, MIN(column_perc) as column_perc
            ORDER BY DBPile_column_perc DESC
                )
            SELECT column_base_type                 AS "Type",
                   as_percentage(DBPile_column_perc) AS "DBPile",
                   as_percentage(Kaggle_column_perc) AS "Kaggle",
                   as_percentage(HF_column_perc) AS "HF",
                   as_percentage(IMDB_column_perc) AS "IMDB",
                   as_percentage(SO_column_perc) AS "SO",
                   as_percentage(TPC_column_perc)  AS "TPC"
            FROM pivoted
            WHERE column_base_type NOT IN ('Boolean', 'OTHER')
            """

    df = con.execute(query).df()
    table_operators = format_df(df, output_format, label, caption)
    return table_operators


def test_get_column_type_table():
    con = get_con(read_only=True)
    print(get_column_type_table(con, 'latex'))  # type: ignore

def create_queries_per_repo_plot(con: duckdb.DuckDBPyConnection) -> str:
    """
    Create a violin plot showing the number of queries per repo
    """

    # Get the number of queries per repo
    df = con.execute("""
                     SELECT r.repo_name, COUNT(*) AS query_count
                     FROM queries q
                              JOIN repos r ON q.repo_id = r.id
                     GROUP BY r.repo_name
                     ORDER BY query_count DESC;
                     """).df()

    # Create the violin plot
    plt.figure(figsize=(10, 6))
    # sns.violinplot(y=df['query_count'])
    plt.title('Distribution of Queries per Repository')
    plt.ylabel('Number of Queries')
    plt.tight_layout()

    # Save the plot
    plot_path = os.path.join(PLOTS_DIR, 'assets', 'queries_per_repo_violin.png')
    if not os.path.exists(os.path.dirname(plot_path)):
        os.makedirs(os.path.dirname(plot_path))
    plt.savefig(plot_path)
    plt.close()

    return plot_path


def get_table_stats(con: duckdb.DuckDBPyConnection) -> str:
    """
    Generate statistics on tables, columns, and values
    """

    # Calculate average tables per repo
    avg_tables_per_repo = con.execute("""
                                      SELECT AVG(table_count) AS avg_tables_per_repo
                                      FROM (SELECT r.id AS repo_id, COUNT(DISTINCT t.id) AS table_count
                                            FROM repos r
                                                     LEFT JOIN tables t ON r.id = t.repo_id
                                            GROUP BY r.id) AS tables_per_repo
                                      """).fetchone()[0]

    # Calculate average columns per table
    avg_columns_per_table = con.execute("""
                                        SELECT AVG(column_count) AS avg_columns_per_table
                                        FROM (SELECT t.id AS table_id, COUNT(c.id) AS column_count
                                              FROM tables t
                                                       LEFT JOIN columns c ON t.id = c.table_id
                                              GROUP BY t.id) AS columns_per_table
                                        """).fetchone()[0]

    # Calculate average values per table
    avg_values_per_table = con.execute("""
                                       SELECT AVG(value_count) AS avg_values_per_table
                                       FROM (SELECT t.id AS table_id, COUNT(cv.value) AS value_count
                                             FROM tables t
                                                      LEFT JOIN columns c ON t.id = c.table_id
                                                      LEFT JOIN column_values cv ON c.id = cv.column_id
                                             GROUP BY t.id) AS values_per_table
                                       """).fetchone()[0]

    # Create a DataFrame with the results
    data = {
        'Average Tables per Repo': [avg_tables_per_repo],
        'Average Columns per Table': [avg_columns_per_table],
        'Average Values per Table': [avg_values_per_table]
    }

    df = pd.DataFrame(data)

    # Create a nice md table
    df = df.round(2)
    md_table_stats = df.to_markdown(index=False, tablefmt="pipe")

    return md_table_stats


def get_value_number_stats(con: duckdb.DuckDBPyConnection) -> str:
    stats = con.execute("""
                        WITH cnts AS (SELECT column_id, COUNT(*) as cnt, COUNT(DISTINCT value) as distinct_cnt
                                      FROM column_values
                                      GROUP BY column_id)
                        SELECT AVG(cnt),
                               MEDIAN(cnt),
                               MIN(cnt),
                               Max(cnt),
                               SUM(cnt > 10),
                               SUM(cnt > 100),
                               SUM(cnt > 1000),
                               AVG(distinct_cnt),
                               MEDIAN(distinct_cnt),
                               MIN(distinct_cnt),
                               Max(distinct_cnt),
                               SUM(distinct_cnt > 10),
                               SUM(distinct_cnt > 100),
                               SUM(distinct_cnt > 1000)
                        FROM cnts;
                        """).df()

    stats = stats.round(2)
    # make a nice table with two columns for distinct and total counts and rows for avg, median, min, max, and sums
    stats.columns = [
        'Average Total Count', 'Median Total Count', 'Min Total Count', 'Max Total Count',
        'Sum > 10 Total Count', 'Sum > 100 Total Count', 'Sum > 1000 Total Count',
        'Average Distinct Count', 'Median Distinct Count', 'Min Distinct Count', 'Max Distinct Count',
        'Sum > 10 Distinct Count', 'Sum > 100 Distinct Count', 'Sum > 1000 Distinct Count'
    ]
    md_table = stats.to_markdown(index=False, tablefmt="pipe")
    return md_table


def get_usages_informations():
    ops = ['JOIN_KEY', 'SCAN_LOOKUP', 'SCAN_FILTER', 'PROJECTION', 'ORDER_KEY', 'GROUP_KEY', 'FILTER', 'AGGREGATE']

    tables = []

    for op in ops:
        con = duckdb.connect(DATABASE_PATH, read_only=True)
        con.execute("""
                    CREATE TEMP VIEW column_usages_unnested AS
                    (
                    SELECT *, unnest(column_ids) AS column_id
                    FROM column_usages
                    )
                    """)
        query = f"""
            WITH usages AS (
              SELECT query_id, repo_id, node_id, usage_type, list(column_base_type) as op_types, COUNT(*) as cnt 
              FROM column_usages_unnested 
              JOIN columns on column_id = columns.id 
              JOIN queries on query_id = queries.id
              WHERE usage_type = '{op}'
              GROUP BY ALL HAVING cnt > 0 
              ORDER BY cnt DESC
            ) 
            SELECT usage_type, list_reduce(op_types, (x, y) -> x || ', ' || y, '')[3:] AS types, 
              COUNT(*) as "# Occurences", COUNT(DISTINCT query_id) as "# Unique Queries", COUNT(DISTINCT repo_id) as "# Unique Repos"
            FROM usages
            GROUP BY ALL
            ORDER BY 3 DESC
            LIMIT 5;
        """
        df = con.execute(query).df()
        df = df.round(2)
        df.columns = ['Usage Type', 'Types', '# Occurrences', '# Unique Queries', '# Unique Repos']
        md_table = df.to_markdown(index=False, tablefmt="pipe")
        md_table = f"## {op}\n\n{md_table}"
        tables.append(md_table)

    return '\n\n'.join(tables)


if __name__ == "__main__":
    con = get_con()

    tb_1, tb_2 = get_query_type_query(con)
    plot_path = create_queries_per_repo_plot(con)
    table_stats = get_table_stats(con)

    path = os.path.join(PLOTS_DIR, 'dataset_stats.md')
    plot_path = plot_path.replace(PLOTS_DIR, '.').replace('\\', '/')

    usage_combinations = get_usages_informations()

    # Write the results to a markdown file
    template = f"""# Operator Statistics
## Query Types
{tb_1}
## Operators
{tb_2}
# Table Statistics
{table_stats}
## Queries per Repository
![Queries per Repository]({plot_path})
# Dataset Statistics
## Value Number Statistics  
{get_value_number_stats()}
# Usage Combinations
{usage_combinations}
"""

    with open(path, 'w') as f:
        f.write(template)
