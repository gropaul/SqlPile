import os
import os.path
from typing import List, Optional

import duckdb
import matplotlib.pyplot as plt
import numpy as np

from src.config import DATABASE_PATH, PLOTS_DIR

# Configure matplotlib to use LaTeX styling if available
try:
    plt.rcParams.update({
        "text.usetex": True,
        "font.family": "serif",
        "font.serif": ["Computer Modern Roman"],
        "axes.labelsize": 12,
        "font.size": 11,
        "legend.fontsize": 10,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "figure.figsize": (12, 8),
        "text.latex.preamble": r"\usepackage{amsmath}"
    })
    print("Using LaTeX for plot typography")
except Exception as e:
    print(f"Could not configure LaTeX typography: {e}")
    # Fallback to a nice non-LaTeX style
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.rcParams.update({
        "font.family": "serif",
        "axes.labelsize": 12,
        "font.size": 11,
        "legend.fontsize": 10,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "figure.figsize": (12, 8)
    })


def column_physical_type_usage_plot(con: duckdb.DuckDBPyConnection, usage_types: List[str]):
    """
    Create a stacked bar plot showing column usage types and their distribution by column base type.
    """

    # Dictionary to store all results
    all_results = get_results(con, usage_types, 'column_base_type')

    # Create stacked bar plot for column counts
    dir = os.path.join(PLOTS_DIR, 'column_usage', 'physical_type')
    create_stacked_bar_plot(all_results, 'column_cnt', dir)
    create_stacked_bar_plot(all_results, 'column_distinct_cnt', dir)
    create_stacked_bar_plot(all_results, 'query_cnt', dir)
    create_stacked_bar_plot(all_results, 'repo_cnt', dir)


def get_results(con: duckdb.DuckDBPyConnection, usage_types: List[str], group_column: str, where_clause: str = "TRUE ",
                join_clause: str = ""):
    all_results = {}

    for usage in usage_types:
        query = f"""
            WITH unnested_ids AS (
                SELECT id, query_id, unnest(column_ids) AS column_id, unifiy_usage_types(usage_type) as usage_type, expression, meta_data
                FROM column_usages
            )
            SELECT 
                usage_type, {group_column}, 
                COUNT(unnested_ids.column_id) as column_cnt, 
                COUNT(DISTINCT unnested_ids.column_id) as column_distinct_cnt, 
                COUNT(DISTINCT query_id) as query_cnt, 
                COUNT(DISTINCT repo_id) as repo_cnt
            FROM unnested_ids
            JOIN columns ON columns.id = unnested_ids.column_id 
            JOIN queries q on q.id = query_id
            JOIN column_usage_history history ON history.column_id = unnested_ids.column_id AND history.usage_id = unnested_ids.id
            {join_clause}
            WHERE 
                usage_type = '{usage}' AND 
                ({where_clause}) AND 
                meta_data.right_table_is_chunk_get is not True AND 
                len(list_distinct(list_transform(history[:-2], x -> x.expression_class))) <= 1
            GROUP BY all 
            ORDER BY usage_type, column_cnt DESC;
        """
        print(query)
        result = con.execute(query).fetchall()
        all_results[usage] = result
    return all_results


def column_semantic_type_sato_usage_plot(con: duckdb.DuckDBPyConnection, usage_types: List[str],
                                         group_column: str = 'semantic_type[6:]',
                                         where_clause: str = "contains(semantic_type, 'sato_')"):
    # Dictionary to store all results
    all_results = get_results(con, usage_types, group_column, where_clause)

    # Create stacked bar plot for column counts

    dir = os.path.join(PLOTS_DIR, 'column_usage', 'semantic_type_sato')
    create_stacked_bar_plot(all_results, 'column_cnt', dir)
    create_stacked_bar_plot(all_results, 'column_distinct_cnt', dir)
    create_stacked_bar_plot(all_results, 'query_cnt', dir)
    create_stacked_bar_plot(all_results, 'repo_cnt', dir)


def darken_color(color, amount=1.3):
    import matplotlib.colors as mcolors
    c = mcolors.to_rgb(color)
    return tuple(max(min(x / amount, 1.0), 0.0) for x in c)


def create_stacked_bar_plot(all_results, count_type, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    """
    Create a stacked bar plot for the given count type, showing percentages.

    Args:
        all_results: Dictionary with usage_type as key and query results as value
        count_type: The type of count to plot ('column_cnt', 'query_cnt', or 'repo_cnt')
        output_dir: Directory to save the plot
    """
    # Map count_type to index in the result tuple
    count_indices = {
        'column_cnt': 2,  # Index of column_cnt in the result tuple
        'column_distinct_cnt': 3,  # Index of column_cnt in the result tuple
        'query_cnt': 4,  # Index of query_cnt in the result tuple
        'repo_cnt': 5  # Index of repo_cnt in the result tuple
    }

    count_index = count_indices[count_type]

    # Collect all unique column_base_types across all usage types
    all_column_types = set()
    for results in all_results.values():
        for result in results:
            all_column_types.add(result[1])  # column_base_type is at index 1

    print(all_column_types)
    all_column_types = sorted(list(all_column_types))

    # Prepare data for stacked bar plot
    usage_types = list(all_results.keys())
    data = {column_type: [0] * len(usage_types) for column_type in all_column_types}

    # Fill in the data with absolute counts first
    for i, usage_type in enumerate(usage_types):
        results = all_results[usage_type]
        for result in results:
            column_type = result[1]  # column_base_type
            count = result[count_index]  # The count value
            data[column_type][i] = count

    # Create a copy of the original absolute counts before converting to percentages
    original_counts = {column_type: data[column_type].copy() for column_type in all_column_types}

    # Calculate total counts for each usage type
    totals = [0] * len(usage_types)
    for i in range(len(usage_types)):
        for column_type in all_column_types:
            totals[i] += data[column_type][i]

    # Convert counts to percentages

    for column_type in all_column_types:
        for i in range(len(usage_types)):
            if totals[i] > 0:  # Avoid division by zero
                data[column_type][i] = (data[column_type][i] / totals[i]) * 100

    average_column_type_usage = {column_type: max(data[column_type]) for column_type in all_column_types}

    top_usages = sorted(average_column_type_usage.items(), key=lambda x: x[1], reverse=True)

    # only take usages with more than 3% usage
    top_usages = [(column_type, usage) for column_type, usage in top_usages if usage > 3]

    # add all the other column types that are not in the top to one "Other" category
    other_column_types = [column_type for column_type in all_column_types if column_type not in dict(top_usages)]
    if other_column_types:
        # Create an "Other" category that sums up the counts of all other column types
        original_counts["Other"] = [sum(original_counts[column_type][i] for column_type in other_column_types) for i in
                                    range(len(usage_types))]
        data["Other"] = [sum(data[column_type][i] for column_type in other_column_types) for i in
                         range(len(usage_types))]

        # Remove the individual "Other" column types from the data
        for column_type in other_column_types:
            del data[column_type]

    # Create the stacked bar plot
    plt.figure(figsize=(9, 6))

    bottom = [0] * len(usage_types)

    # Create a colormap with distinct and aesthetically pleasing colors
    # Using a custom colormap for better aesthetics
    # colors = plt.cm.plasma(np.linspace(0, 0.9, len(data.keys()) - (1 if "Other" in data else 0)))
    colors = [
        "#0072B2",  # Blue
        "#D55E00",  # Vermilion
        "#56B4E9",  # Sky Blue
        "#E69F00",  # Orange
        "#009E73",  # Bluish Green
        "#F0E442",  # Yellow
        "#CC79A7",  # Reddish Purple
        "#000000",  # Black
    ]

    # Define hatching patterns with lower density for reduced opacity effect
    hatches = ['..', '++', 'xx', 'oo', '**', '--', '||', '//']

    # Get the column types from the data dictionary (after removing small ones)
    column_types_to_plot = list(data.keys())

    for i, column_type in enumerate(column_types_to_plot):
        color = 'gray' if column_type == "Other" else colors[i % len(colors)]
        # Use modulo to cycle through hatches if there are more column types than hatch patterns
        hatch = '//' if column_type == "Other" else hatches[i % len(hatches)]
        hatch_color = darken_color(color, 1.3) if hatch else color

        # Create the bar segment
        bars = plt.bar(usage_types, data[column_type], bottom=bottom, label=column_type,
                       color=color, hatch=hatch, edgecolor=hatch_color, linewidth=0.3)
        plt.bar(usage_types, data[column_type], bottom=bottom,
                color='none', edgecolor='black', linewidth=0.5)

        # Add text labels with absolute counts in the middle of each segment
        for j, rect in enumerate(bars):
            # Calculate the vertical position for the text (middle of the segment)
            height = rect.get_height()
            y_pos = rect.get_y() + height / 2

            # Get the absolute count for this segment
            count = original_counts[column_type][j]

            # Only add label if count is significant (to avoid cluttering)
            if count > 0:
                # Format the count as an integer
                count_text = f"{int(count)}"

                # Add the text label
                # plt.text(j, y_pos, count_text, ha='center', va='center',
                #          fontsize=15, color='black', fontweight='bold')

        # Update the bottom position for the next segment
        bottom = [bottom[j] + data[column_type][j] for j in range(len(usage_types))]

    # Add labels and title
    plt.xlabel('Usage Type')
    plt.ylabel(f'Percentage ({count_type})')
    plt.title(f'Column Usage Types by {count_type} (Percentage)')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Column Base Type', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    # Save the plot
    filename = f'usage_types_by_{count_type}.{OUTPUT_FORMAT}'
    plt.savefig(os.path.join(output_dir, filename), format=OUTPUT_FORMAT, bbox_inches='tight')
    plt.close()

    print(f"Saved stacked bar plot for {count_type} to {os.path.join(output_dir, filename)}")


def column_semantic_type_syntactic_usage_plot(con: duckdb.DuckDBPyConnection, usage_types: List[str]):
    """
    Create a stacked bar plot showing column semantic types and their distribution by syntactic type.
    """

    # Dictionary to store all results
    all_results = get_results(con, usage_types, "ifnull(semantic_type_syntactic, 'Other') as semantic_type_syntactic",
                              "(semantic_type_syntactic != 'Test' or semantic_type_syntactic is null) and column_base_type = 'Text'")

    # Create stacked bar plot for column counts
    dir = os.path.join(PLOTS_DIR, 'column_usage', 'semantic_type_syntactic')
    create_stacked_bar_plot(all_results, 'column_cnt', dir)
    create_stacked_bar_plot(all_results, 'column_distinct_cnt', dir)
    create_stacked_bar_plot(all_results, 'query_cnt', dir)
    create_stacked_bar_plot(all_results, 'repo_cnt', dir)


def unify_llm_type(semantic_type: Optional[str]) -> str:

    # {None, '', 'Password', 'Identifier', 'Contact', 'Boolean', 'Numeric', 'URL', 'Location',
    # '', 'DateTime', 'Category', 'Email', 'Name', 'PhoneNumber', 'FullText', 'Title', 'Function'}

    # Gender, Color are transformed to 'Category'
    if semantic_type in ['Gender', 'Color']:
        return 'Category'

    # Email, PhoneNumber are transformed to 'Contact'
    if semantic_type in ['Email', 'PhoneNumber']:
        return 'Contact'

    return  semantic_type if semantic_type else 'Unknown'


def column_semantic_type_llm_usage_plot(con: duckdb.DuckDBPyConnection, usage_types: List[str]):
    """
    Create a stacked bar plot showing column semantic types and their distribution by LLM usage.
    """
    con.create_function('unify_llm_type', unify_llm_type, null_handling='SPECIAL')
    # Dictionary to store all results
    all_results = get_results(
        con, usage_types,
        "unify_llm_type(semantic_type_llm) as semantic_type_llm",
        "(semantic_type_llm != 'Test' or semantic_type_llm is null) and column_base_type = 'Text'",
        "JOIN (SELECT column_id as column_id_llm, semantic_type as semantic_type_llm FROM '/Users/paul/workspace/SqlPile/src/data_analysis/semantic_types.csv') AS st ON st.column_id_llm = columns.id"
    )

    # Create stacked bar plot for column counts
    dir = os.path.join(PLOTS_DIR, 'column_usage', 'semantic_type_llm')
    create_stacked_bar_plot(all_results, 'column_cnt', dir)
    create_stacked_bar_plot(all_results, 'column_distinct_cnt', dir)
    create_stacked_bar_plot(all_results, 'query_cnt', dir)
    create_stacked_bar_plot(all_results, 'repo_cnt', dir)


OUTPUT_FORMAT = 'png'


def unifiy_usage_types(usage_type: str) -> str:
    if usage_type == 'TOP_N_KEY':
        return 'ORDER_KEY'

    if usage_type == 'SCAN_FILTER':
        return 'FILTER'

    if usage_type == 'DISTINCT_KEY':
        return 'GROUP_KEY'

    return usage_type


if __name__ == "__main__":
    con = duckdb.connect(DATABASE_PATH, read_only=True)
    con.create_function(
        "unifiy_usage_types",
        unifiy_usage_types,
        null_handling='SPECIAL',
    )
    usage_types = con.execute("""SELECT DISTINCT unifiy_usage_types(usage_type)
                                 FROM column_usages
                                 ORDER BY usage_type""").fetchall()
    usage_types = [usage[0] for usage in usage_types]
    print(f"Found {len(usage_types)} usage types: {usage_types}")

    column_physical_type_usage_plot(con, usage_types)
    column_semantic_type_sato_usage_plot(con, usage_types)
    column_semantic_type_llm_usage_plot(con, usage_types)
    column_semantic_type_syntactic_usage_plot(con, usage_types)

