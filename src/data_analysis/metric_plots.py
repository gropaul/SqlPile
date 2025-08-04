import math
import os
from collections import Counter

import duckdb
import matplotlib.pyplot as plt
import numpy as np

from src.config import DATABASE_PATH, PLOTS_DIR
from src.data_analysis.usage_plots import unifiy_usage_types


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


def filter_outliers(data_list, lower_percentile=2, upper_percentile=98):
    """
    Filter outliers from a list of data points based on percentiles.

    Args:
        data_list: List of numeric values
        lower_percentile: Lower percentile threshold (default: 5)
        upper_percentile: Upper percentile threshold (default: 95)

    Returns:
        Filtered list with outliers removed
    """
    if not data_list or len(data_list) < 10:  # Don't filter if too few data points
        return data_list

    try:
        # Calculate percentile thresholds
        lower_bound = np.percentile(data_list, lower_percentile)
        upper_bound = np.percentile(data_list, upper_percentile)

        # Filter values within the percentile range
        return [x for x in data_list if lower_bound <= x <= upper_bound]
    except Exception as e:
        print(f"Warning: Could not filter outliers: {e}")
        return data_list


def create_metric_plots_by_usage_type():
    """
    Create violin plots showing the distribution of metrics for columns with each usage_type.
    For each metric (like min_value_length, max_value_length, etc.), create a violin plot
    with usage types on the x-axis and the metric values on the y-axis.
    Save each plot as a separate PDF file.
    """
    con = duckdb.connect(DATABASE_PATH, read_only=False)
    # con = duckdb.connect('/Users/paul/workspace/SqlPile/data/schemapile_29_07.duckdb')

    # Register the normalized_entropy function with DuckDB
    con.create_function("normalized_entropy", normalized_entropy, [str], float, type="native")

    con.create_function(
        "unifiy_usage_types",
        unifiy_usage_types,
        null_handling='SPECIAL',
    )

    # First, make sure the values_often view exists
    con.execute("""
                CREATE OR REPLACE VIEW values_often AS
                WITH often AS (SELECT column_id
                               FROM column_values
                               GROUP BY column_id
                               HAVING COUNT(DISTINCT value) > 5
                                   OR COUNT(value) > 10)
                SELECT column_values.column_id, column_values.value
                FROM column_values
                WHERE column_values.column_id IN (SELECT column_id FROM often)
                  AND value != 'example text'
                  and value != 'None'
                  and len(value) > 0
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
        -- CREATE TABLE IF NOT EXISTS value_stats AS
        WITH usages AS (
                SELECT unnest(column_ids) as column_id, unifiy_usage_types(usage_type) as usage_type, expression
                FROM column_usages
            ),
        usages_aggs AS (
          PIVOT usages
          ON usage_type
          USING ifnull(LIST(expression), []) as USAGE_EXPRESSIONS
        ), 
        value_aggs AS (
          SELECT 
            column_id,
            COUNT(DISTINCT value) AS distinct_value_count,
            COUNT(value)          AS total_value_count,
            
            AVG(bit_length(value) // 8) AS avg_bytes_per_string,
            MIN(bit_length(value) // 8) AS min_bytes_per_string,
            MAX(bit_length(value) // 8) AS max_bytes_per_string,
            
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
            list(DISTINCT value)[:10] as values
          FROM values_often 
          WHERE len(value) > 0
          GROUP BY column_id
        )
        SELECT columns.id, semantic_type_llm as semantic_type, columns.column_name AS column_name, tables.table_name AS table_name, 
          usages_aggs.*, 
          value_aggs.*
        FROM value_aggs
        JOIN usages_aggs ON value_aggs.column_id = usages_aggs.column_id
        JOIN columns ON value_aggs.column_id = columns.id
        JOIN tables ON columns.table_id = tables.id
        LEFT JOIN (
           SELECT column_id as column_id_llm, semantic_type as semantic_type_llm 
           FROM '/Users/paul/workspace/SqlPile/src/data_analysis/semantic_types.csv'
         ) AS st ON st.column_id_llm = columns.id
          ORDER BY value_aggs.column_id;
    """)

    print("Value stats view created successfully.")

    # Get all distinct usage types
    usage_types = con.execute("""SELECT DISTINCT unifiy_usage_types(usage_type) FROM column_usages""").fetchall()
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
                SELECT vs.{metric}, len(COLUMNS(c -> c LIKE '%_USAGE_EXPRESSIONS')) as usage_expressions_count
                FROM value_stats vs
                WHERE len({usage_type}_USAGE_EXPRESSIONS) > 0
            """
            result = con.execute(query).fetchall()

            # Convert to a list of values
            values = [row[0] for row in result if row[0] is not None]

            # Filter outliers to prevent them from distorting the plot
            filtered_values = filter_outliers(values)
            data.append(filtered_values)

        # Filter out empty data and adjust usage types accordingly
        filtered_data_with_labels = [(d, label) for d, label in zip(data, usage_types) if len(d) > 0]
        filtered_data, filtered_labels = zip(*filtered_data_with_labels)

        # Create the violin plot with filtered data
        if filtered_data:
            ax.violinplot(
                filtered_data,
                showmeans=True,
                showmedians=True,
                showextrema=False
            )
            ax.set_xticks(range(1, len(filtered_labels) + 1))
            ax.set_xticklabels(filtered_labels)

            # Set reasonable y-axis limits based on the filtered data
            # Combine all filtered data to calculate global min and max
            all_values = [val for sublist in data for val in sublist]
            if all_values:
                # Use percentiles to set limits to avoid any remaining outliers
                y_min = np.percentile(all_values, 1)
                y_max = np.percentile(all_values, 99)

                # Add some padding
                y_range = y_max - y_min
                y_min = max(0, y_min - 0.05 * y_range)  # Ensure non-negative for counts
                y_max = y_max + 0.05 * y_range

                ax.set_ylim(y_min, y_max)

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
