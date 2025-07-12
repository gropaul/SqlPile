import os.path
import os
import matplotlib.pyplot as plt
import duckdb
import matplotlib as mpl
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


def column_usage_plot():
    """
    Create a stacked bar plot showing column usage types and their distribution by column base type.
    """
    con = duckdb.connect(DATABASE_PATH)
    usage_types = con.execute("""SELECT DISTINCT usage_type FROM column_usages""").fetchall()

    dir = os.path.join(PLOTS_DIR, 'column_usage')
    if not os.path.exists(dir):
        os.makedirs(dir)

    # Dictionary to store all results
    all_results = {}

    for (usage, ) in usage_types:
        query = f"""
            WITH unnested_ids AS (
                SELECT id, query_id, unnest(column_ids) AS c_id, usage_type
                FROM column_usages
            )
            SELECT 
                usage_type, column_base_type, 
                COUNT(c_id) as column_cnt, 
                COUNT(DISTINCT c_id) as column_distinct_cnt, 
                COUNT(DISTINCT query_id) as query_cnt, 
                COUNT(DISTINCT repo_id) as repo_cnt
            FROM unnested_ids
            JOIN columns ON columns.id = unnested_ids.c_id JOIN queries q on q.id = query_id
            WHERE usage_type = '{usage}'
            GROUP BY all ORDER BY usage_type, column_cnt DESC;
        """

        result = con.execute(query).fetchall()
        all_results[usage] = result
        print(f"Results for {usage}:")
        print(result)

    # Create stacked bar plot for column counts
    create_stacked_bar_plot(all_results, 'column_cnt', dir)

    create_stacked_bar_plot(all_results, 'column_distinct_cnt', dir)

    # Create stacked bar plot for query counts
    create_stacked_bar_plot(all_results, 'query_cnt', dir)

    # Create stacked bar plot for repo counts
    create_stacked_bar_plot(all_results, 'repo_cnt', dir)


def darken_color(color, amount=1.3):
    """
    Darken the given color by multiplying its RGB components by 1/amount.

    Args:
        color: Matplotlib color (e.g., from a colormap)
        amount: Factor by which to darken the color

    Returns:
        Darkened RGB color tuple
    """
    import matplotlib.colors as mcolors
    c = mcolors.to_rgb(color)
    return tuple(max(min(x / amount, 1.0), 0.0) for x in c)


def create_stacked_bar_plot(all_results, count_type, output_dir):
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
        'query_cnt': 4,   # Index of query_cnt in the result tuple
        'repo_cnt': 5     # Index of repo_cnt in the result tuple
    }

    count_index = count_indices[count_type]

    # Collect all unique column_base_types across all usage types
    all_column_types = set()
    for results in all_results.values():
        for result in results:
            all_column_types.add(result[1])  # column_base_type is at index 1

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

    # Group column types with percentages < 3% into "Other" category
    other_data = [0] * len(usage_types)
    other_original_counts = [0] * len(usage_types)
    small_column_types = []

    for column_type in all_column_types:
        is_small = True
        for i in range(len(usage_types)):
            if data[column_type][i] >= 3:
                is_small = False
                break

        if is_small:
            small_column_types.append(column_type)
            for i in range(len(usage_types)):
                other_data[i] += data[column_type][i]
                other_original_counts[i] += original_counts[column_type][i]

    # Remove small column types from the data dictionary and original_counts
    for column_type in small_column_types:
        del data[column_type]
        del original_counts[column_type]

    # Add "Other" category if there are any small column types
    if small_column_types:
        data["Other"] = other_data
        original_counts["Other"] = other_original_counts

    # Create the stacked bar plot
    plt.figure(figsize=(12, 8))

    bottom = [0] * len(usage_types)

    # Create a colormap with distinct and aesthetically pleasing colors
    # Using a custom colormap for better aesthetics
    colors = plt.cm.plasma(np.linspace(0, 0.9, len(data.keys()) - (1 if "Other" in data else 0)))

    # Define hatching patterns with lower density for reduced opacity effect
    hatches = ['..', '++', 'xx', 'oo', '**', '--', '||', '//']

    # Get the column types from the data dictionary (after removing small ones)
    column_types_to_plot = list(data.keys())

    # Move "Other" to the end if it exists
    if "Other" in column_types_to_plot:
        column_types_to_plot.remove("Other")
        column_types_to_plot.append("Other")

    for i, column_type in enumerate(column_types_to_plot):
        color = 'gray' if column_type == "Other" else colors[i]
        # Use modulo to cycle through hatches if there are more column types than hatch patterns
        hatch = '' if column_type == "Other" else hatches[i % len(hatches)]
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
                plt.text(j, y_pos, count_text, ha='center', va='center', 
                         fontsize=15, color='black', fontweight='bold')

        # Update the bottom position for the next segment
        bottom = [bottom[j] + data[column_type][j] for j in range(len(usage_types))]

    # Add labels and title
    plt.xlabel('Usage Type')
    plt.ylabel(f'Percentage ({count_type})')
    plt.title(f'Column Usage Types by {count_type} (Percentage)')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Column Base Type', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    # Save the plot as PDF with high quality
    filename = f'usage_types_by_{count_type}.pdf'
    plt.savefig(os.path.join(output_dir, filename), format='pdf', bbox_inches='tight', dpi=300)
    plt.close()

    print(f"Saved stacked bar plot for {count_type} to {os.path.join(output_dir, filename)}")


if __name__ == "__main__":
    column_usage_plot()
