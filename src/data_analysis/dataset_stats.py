import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

from src.config import PLOTS_DIR, DATABASE_PATH


def usage_type_to_operator(usage_type: str) -> str:
    usage_to_operator_map = {
        'TOP_N_KEY': 'ORDER BY',
        'SCAN_LOOKUP': 'SCAN',
        'SCAN_FILTER': 'SCAN_FILTER',
        'PROJECTION': 'PROJECTION',
        'ORDER_KEY': 'ORDER BY',
        'JOIN_KEY': 'JOIN',
        'GROUP_KEY': 'GROUP',
        'FILTER': 'FILTER',
        'AGGREGATE': 'AGGREGATE'
    }

    if usage_type not in usage_to_operator_map:
        raise ValueError(
            f"Unknown usage type: '{usage_type}'. Known types: {list(usage_to_operator_map.keys())}"
        )

    return usage_to_operator_map[usage_type]


def get_operator_stats():
    """
    Create a view with the number of operators in a query
    """

    con = duckdb.connect(DATABASE_PATH, read_only=True)
    con.create_function("usage_type_to_operator", usage_type_to_operator, [str], str, type="native")

    # get the number of queries per type
    df = con.execute("""
                     SELECT type as "Type", COUNT(*) AS "Count"
                     FROM queries
                     GROUP BY type
                     ORDER BY 2 DESC;
                     """).df()

    # create a nice md table
    df = df.round(2)
    md_table_query_types = df.to_markdown(index=False, tablefmt="pipe")

    df = con.execute("""
                     WITH ops AS (SELECT query_id, node_id, usage_type_to_operator(usage_type) AS op
                                  FROM column_usages
                                  GROUP BY query_id, node_id, usage_type),
                          op_counts AS (SELECT query_id, op, COUNT(*) as op_count
                                        FROM ops
                                        GROUP BY query_id, op),
                          queries_count AS (SELECT COUNT(DISTINCT query_id) as query_count
                                            FROM op_counts)
                     SELECT oc.op                             as "Operator",
                            SUM(oc.op_count)                  as "Total Operator Count",
                            COUNT(DISTINCT oc.query_id)       as "Query Count with Operator",
                            SUM(oc.op_count) / qc.query_count as "Avg Count per Query"
                     FROM op_counts oc
                              CROSS JOIN queries_count qc
                     GROUP BY oc.op, qc.query_count
                     ORDER BY #4 DESC;
                     """).df()

    # create a nice md table
    df = df.round(2)
    md_table_operators = df.to_markdown(index=False, tablefmt="pipe")

    return md_table_query_types, md_table_operators


def create_queries_per_repo_plot() -> str:
    """
    Create a violin plot showing the number of queries per repo
    """
    con = duckdb.connect(DATABASE_PATH, read_only=True)

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
    sns.violinplot(y=df['query_count'])
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


def get_table_stats():
    """
    Generate statistics on tables, columns, and values
    """
    con = duckdb.connect(DATABASE_PATH, read_only=True)

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


def get_value_number_stats():
    con = duckdb.connect(DATABASE_PATH, read_only=True)

    stats = con.execute("""
                        WITH cnts AS (
                            SELECT column_id, COUNT(*) as cnt, COUNT(DISTINCT  value) as distinct_cnt
                            FROM column_values 
                            GROUP BY column_id
                        )
                            SELECT 
                                AVG(cnt), 
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


def get_usage_combinations():

    ops = ['JOIN_KEY', 'SCAN_LOOKUP', 'SCAN_FILTER', 'PROJECTION', 'ORDER_KEY', 'GROUP_KEY', 'FILTER', 'AGGREGATE']

    tables = []

    for op in ops:
        con = duckdb.connect(DATABASE_PATH, read_only=True)
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
            LIMIT 10
        """
        df = con.execute(query).df()
        df = df.round(2)
        df.columns = ['Usage Type', 'Types', '# Occurrences', '# Unique Queries', '# Unique Repos']
        md_table = df.to_markdown(index=False, tablefmt="pipe")
        md_table = f"## {op}\n\n{md_table}"
        tables.append(md_table)

    return '\n\n'.join(tables)


if __name__ == "__main__":
    tb_1, tb_2 = get_operator_stats()
    plot_path = create_queries_per_repo_plot()
    table_stats = get_table_stats()

    path = os.path.join(PLOTS_DIR, 'dataset_stats.md')
    plot_path = plot_path.replace(PLOTS_DIR, '').replace('\\', '/')

    usage_combinations = get_usage_combinations()

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
