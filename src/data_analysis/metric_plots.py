import os
import matplotlib.pyplot as plt
import duckdb
import math
from collections import Counter

from src.config import DATABASE_PATH, PLOTS_DIR


def normalized_entropy(s: str) -> float:
    """Calculate the normalized Shannon entropy of a string."""
    if not s:
        return 0.0

    counts = Counter(s)
    total = len(s)
    k = len(counts)

    shannon_entropy = -sum((count / total) * math.log2(count / total) for count in counts.values())
    max_entropy = math.log2(k) if k > 1 else 1.0  # Avoid division by 0

    return shannon_entropy / max_entropy


import regex as re  # pip install regex (not re)


def create_metric_plots_by_usage_type():
    """
    Create box plots showing the distribution of metrics for columns with each usage_type.
    For each metric (like min_value_length, max_value_length, etc.), create a box plot
    with usage types on the x-axis and the metric values on the y-axis.
    Save each plot as a separate PDF file.
    """
    con = duckdb.connect(DATABASE_PATH)

    # Register the normalized_entropy function with DuckDB
    con.create_function("normalized_entropy", normalized_entropy, [str], float, type="native")

    # First, make sure the values_often view exists
    con.execute("""
                CREATE OR REPLACE VIEW values_often AS
                WITH often AS (SELECT column_id
                               FROM column_values
                               GROUP BY column_id
                               HAVING COUNT(DISTINCT value) > 10
                                   OR COUNT(value) > 10)
                SELECT column_values.column_id, column_values.value
                FROM column_values
                WHERE column_values.column_id IN (SELECT column_id FROM often)
                  AND value != 'example text'
                  and value != 'None'
                ;
                """)

    count = con.execute("""
                        SELECT COUNT(*)
                        FROM values_often
                        """).fetchone()[0]
    print(f"Number of values in values_often: {count}")

    # Now create the value_stats view
    con.execute("""
        CREATE OR REPLACE TABLE value_stats AS
        WITH usages AS (
            SELECT unnest(column_ids) as column_id, usage_type
            FROM column_usages
        )
        SELECT values_often.column_id, MIN(column_name) AS column_name, MIN(table_name) AS table_name,
               COUNT(DISTINCT value) AS distinct_value_count,
               COUNT(value)          AS total_value_count,
               AVG(LENGTH(value))    AS avg_value_length,
               MIN(LENGTH(value))    AS min_value_length,
               MAX(LENGTH(value))    AS max_value_length,
               MAX(LENGTH(value)) - MIN(LENGTH(value)) AS value_length_range,
               COUNT(DISTINCT value) * 1.0 / COUNT(value) AS distinct_ratio,
                AVG(
                  LENGTH(REGEXP_REPLACE(value, '[^a-zA-Z]', '', 'g'))::float 
                  / NULLIF(LENGTH(value), 0)
                ) AS alpha_ratio,
                AVG(
                  LENGTH(REGEXP_REPLACE(value, '[^0-9]', '', 'g'))::float 
                  / NULLIF(LENGTH(value), 0)
                ) AS numeric_ratio,
                AVG(
                  LENGTH(REGEXP_REPLACE(value, '[a-zA-Z0-9]', '', 'g'))::float 
                  / NULLIF(LENGTH(value), 0)
                ) AS special_char_ratio,
                AVG(
                    LENGTH(TRIM(value)) - LENGTH(REPLACE(TRIM(value), ' ', '')) + 1
                ) AS word_count,
               -- normalized_entropy(string_agg(value)) AS normalized_entropy,
               list_distinct(list(usage_type)) AS usage_types
               -- list(value)[0:10] AS sample_values,
               -- list_distinct(list(value)) AS distinct_values
        FROM values_often
        JOIN usages ON values_often.column_id = usages.column_id
        JOIN columns ON values_often.column_id = columns.id
        JOIN tables ON columns.table_id = tables.id
        GROUP BY values_often.column_id;
    """)

    print("Value stats view created successfully.")

    # Get all distinct usage types
    usage_types = con.execute("""
                              SELECT DISTINCT usage_type
                              FROM column_usages
                              """).fetchall()
    usage_types = [ut[0] for ut in usage_types]

    # Define the metrics we want to plot
    metrics = [
        'distinct_value_count',
        'total_value_count',
        'avg_value_length',
        'min_value_length',
        'max_value_length',
        'value_length_range',
        'distinct_ratio',
        'alpha_ratio',
        'special_char_ratio',
        'numeric_ratio',
        # 'normalized_entropy',
        'word_count'
    ]

    # Create directory for plots if it doesn't exist
    plot_dir = os.path.join(PLOTS_DIR, 'metric_plots')
    if not os.path.exists(plot_dir):
        os.makedirs(plot_dir)

    # For each metric, create a plot showing the distribution for each usage_type
    for metric in metrics:
        print(f"Creating plot for {metric}...")

        # Create a single figure for this metric
        fig, ax = plt.subplots(figsize=(15, 8))
        fig.suptitle(f'Distribution of {metric} by Usage Type', fontsize=16)

        # Collect data for all usage types
        data = []
        for usage_type in usage_types:
            # Query the data for this usage type
            query = f"""
                SELECT vs.{metric}
                FROM value_stats vs
                WHERE array_contains(vs.usage_types, '{usage_type}')
            """
            result = con.execute(query).fetchall()

            # Convert to a list of values
            values = [row[0] for row in result if row[0] is not None]
            data.append(values)

        # Create the box plot with all usage types
        if any(data):  # Check if there's any data to plot
            ax.boxplot(
                data,
                labels=usage_types,
                showfliers=False,  # This hides the outliers
            )

            ax.set_xlabel('Usage Type')
            ax.set_ylabel(f'{metric} Value Range')

        plt.tight_layout()

        # Save the current figure as a separate PDF file
        pdf_path = os.path.join(plot_dir, f'metric_plot_{metric}.pdf')
        plt.savefig(pdf_path, format='pdf', bbox_inches='tight', dpi=300)
        plt.close(fig)
        print(f"Plot for {metric} saved to {pdf_path}")

    print(f"All plots saved to {plot_dir}")


if __name__ == "__main__":
    create_metric_plots_by_usage_type()
