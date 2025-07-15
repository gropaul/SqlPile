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

    con = duckdb.connect(DATABASE_PATH)
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
                       SELECT oc.op                      as "Operator",
                              SUM(oc.op_count)            as "Total Operator Count",
                              COUNT(DISTINCT oc.query_id) as "Query Count with Operator",
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
    con = duckdb.connect(DATABASE_PATH)

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
    con = duckdb.connect(DATABASE_PATH)

    # Calculate average tables per repo
    avg_tables_per_repo = con.execute("""
        SELECT AVG(table_count) AS avg_tables_per_repo
        FROM (
            SELECT r.id AS repo_id, COUNT(DISTINCT t.id) AS table_count
            FROM repos r
            LEFT JOIN tables t ON r.id = t.repo_id
            GROUP BY r.id
        ) AS tables_per_repo
    """).fetchone()[0]

    # Calculate average columns per table
    avg_columns_per_table = con.execute("""
        SELECT AVG(column_count) AS avg_columns_per_table
        FROM (
            SELECT t.id AS table_id, COUNT(c.id) AS column_count
            FROM tables t
            LEFT JOIN columns c ON t.id = c.table_id
            GROUP BY t.id
        ) AS columns_per_table
    """).fetchone()[0]

    # Calculate average values per table
    avg_values_per_table = con.execute("""
        SELECT AVG(value_count) AS avg_values_per_table
        FROM (
            SELECT t.id AS table_id, COUNT(cv.value) AS value_count
            FROM tables t
            LEFT JOIN columns c ON t.id = c.table_id
            LEFT JOIN column_values cv ON c.id = cv.column_id
            GROUP BY t.id
        ) AS values_per_table
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


if __name__ == "__main__":
    tb_1, tb_2 = get_operator_stats()
    plot_path = create_queries_per_repo_plot()
    table_stats = get_table_stats()

    path = os.path.join(PLOTS_DIR, 'dataset_stats.md')
    plot_path = plot_path.replace(PLOTS_DIR, '').replace('\\', '/')

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
"""

    with open(path, 'w') as f:
        f.write(template)
