import os
import os.path
from typing import List, Optional

import duckdb
import matplotlib.pyplot as plt
import numpy as np

from src.config import DATABASE_PATH, PLOTS_DIR, get_con

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


def column_physical_type_usage_plot(con: duckdb.DuckDBPyConnection, output_dir: str = PLOTS_DIR):
    """
    Create a stacked bar plot showing column usage types and their distribution by column base type.
    """

    # Dictionary to store all results
    all_results = get_results(con, 'column_base_type')

    # Create stacked bar plot for column counts
    dir = os.path.join(output_dir, 'column_usage', 'physical_type')
    create_stacked_bar_plot(all_results, 'column_cnt', dir)
    # create_stacked_bar_plot(all_results, 'column_distinct_cnt', dir)
    # create_stacked_bar_plot(all_results, 'query_cnt', dir)
    # create_stacked_bar_plot(all_results, 'repo_cnt', dir)


def get_usage_types(con: duckdb.DuckDBPyConnection) -> List[str]:
    usage_types = con.execute("""SELECT DISTINCT unifiy_usage_types(usage_type)
                                 FROM column_usages
                                 ORDER BY usage_type""").fetchall()
    usage_types = [usage[0] for usage in usage_types]
    print(f"Found {len(usage_types)} usage types: {usage_types}")
    return usage_types


def get_data_for_usage(con: duckdb.DuckDBPyConnection, group_column: str, where_clause: str = "TRUE ",
                       join_clause: str = "", usage: Optional[str] = None) -> str:
    query = f"""
                WITH unnested_ids AS (
                    SELECT id, query_id, unnest(column_ids) AS column_id, unifiy_usage_types(usage_type) as usage_type, expression, meta_data
                    FROM column_usages
                ), 
               base AS (
                  SELECT
                    ui.usage_type,
                    get_repo_origin(r.repo_url) AS data_source,
                    ui.column_id,
                    q.id AS query_id,
                    q.repo_id,
                    {group_column} as group_column
                  FROM unnested_ids ui
                  JOIN columns c ON c.id = ui.column_id
                  JOIN queries q ON q.id = ui.query_id
                  JOIN column_usage_history h
                    ON h.column_id = ui.column_id AND h.usage_id = ui.id
                  JOIN repos r ON q.repo_id = r.id
                  {join_clause}
                  WHERE
                    {f"ui.usage_type = '{usage}'" if usage else "TRUE"}
                    AND ({where_clause})
                    AND ui.meta_data.right_table_is_chunk_get IS NOT TRUE
                    AND len(list_distinct(list_transform(history[:-2], x -> x.expression_class))) <= 1
                    AND q.id NOT IN (SELECT query_id FROM column_usages_outliers)
                ),
                counts AS (
                  SELECT
                    {"b.usage_type," if usage else ""}  -- Include usage_type only if specified
                    b.data_source,
                    group_column,
                    COUNT(b.column_id) AS column_cnt,                    
                    COUNT(DISTINCT b.column_id) AS column_distinct_cnt,
                    COUNT(DISTINCT b.query_id)  AS query_cnt,
                    COUNT(DISTINCT b.repo_id)   AS repo_cnt
                  FROM base b
                  GROUP BY ALL
                ),
                total_count_per_source AS (
                  SELECT
                    data_source,
                    COUNT(column_id)                      AS total_column_cnt,  
                    COUNT(DISTINCT column_id)     AS total_column_distinct_cnt,
                    COUNT(DISTINCT query_id)      AS total_query_cnt,
                    COUNT(DISTINCT repo_id)       AS total_repo_cnt
                  FROM base
                  GROUP BY data_source
                )
                SELECT
                  {"c.usage_type" if usage else "'All' AS usage_type"},
                  c.data_source,
                  group_column,
                  1.0 * c.column_cnt          / NULLIF(t.total_column_cnt, 0)          AS column_percentage,
                  1.0 * c.column_distinct_cnt / NULLIF(t.total_column_distinct_cnt, 0)  AS column_distinct_percentage,
                  1.0 * c.query_cnt           / NULLIF(t.total_query_cnt, 0)            AS query_percentage,
                  1.0 * c.repo_cnt            / NULLIF(t.total_repo_cnt, 0)             AS repo_percentage,
                  c.column_cnt,
                c.column_distinct_cnt,
                c.query_cnt,
                c.repo_cnt,
                t.total_column_cnt,
                t.total_column_distinct_cnt,
                t.total_query_cnt,
                t.total_repo_cnt
                FROM counts c
                LEFT JOIN total_count_per_source t USING (data_source);
            """

    result = con.execute(query).fetchall()
    return result


def get_results(con: duckdb.DuckDBPyConnection, group_column: str, where_clause: str = "TRUE ",
                join_clause: str = ""):
    all_results = {}

    usage_types = get_usage_types(con)

    # create a view that contains the filtered column usages
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE column_usages_outliers AS
        WITH usages_unnested AS (
          SELECT query_id, unnest(column_ids) as column_id, usage_type FROM  column_usages
        ), query_counts AS (
          SELECT query_id, usage_type, COUNT(*) as cnt FROM usages_unnested
          JOIN columns on columns.id = usages_unnested.column_id
          GROUP BY query_id, usage_type
          ORDER BY 3 DESC
        ), usage_bounds AS (
          SELECT usage_type, quantile_cont(cnt, 0.9995) upper_bound, max(cnt)  as max
          FROM query_counts
          GROUP BY usage_type 
          ORDER BY 3 DESC
        )
        SELECT query_id FROM query_counts
        JOIN usage_bounds ON usage_bounds.usage_type = query_counts.usage_type
        JOIN queries ON queries.id = query_counts.query_id
        JOIN repos ON repos.id = queries.repo_id
        WHERE 
            cnt > upper_bound -- This means it is an outlier
            AND get_repo_origin(repos.repo_url) = 'SqlPile' -- Only filter for SqlPile
        ORDER BY cnt DESC
    """)

    for usage in usage_types:
        usage_result = get_data_for_usage( con, group_column, where_clause, join_clause, usage)
        all_results[usage] = usage_result

    # create one result for all usage types
    all_results['All'] = get_data_for_usage( con, group_column, where_clause, join_clause, None)

    return all_results


def column_semantic_type_sato_usage_plot(con: duckdb.DuckDBPyConnection,
                                         group_column: str = 'semantic_type[6:]',
                                         where_clause: str = "contains(semantic_type, 'sato_')"):
    # Dictionary to store all results
    all_results = get_results(con, group_column, where_clause)

    # Create stacked bar plot for column counts

    dir = os.path.join(PLOTS_DIR, 'column_usage', 'semantic_type_sato')
    create_stacked_bar_plot(all_results, 'column_cnt', dir)
    # create_stacked_bar_plot(all_results, 'column_distinct_cnt', dir) # everything distinct does not make sense as there is no percentage of distinct useages
    # create_stacked_bar_plot(all_results, 'query_cnt', dir)
    # create_stacked_bar_plot(all_results, 'repo_cnt', dir)


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
        'column_cnt': 3,  # Index of column_cnt in the result tuple
        'column_distinct_cnt': 4,  # Index of column_cnt in the result tuple
        'query_cnt': 5,  # Index of query_cnt in the result tuple
        'repo_cnt': 6  # Index of repo_cnt in the result tuple
    }

    percentage_index = count_indices[count_type]

    # Collect all unique column_base_types across all usage types
    all_group_types = set()
    all_data_sources = set()
    for results in all_results.values():
        for result in results:
            all_data_sources.add(result[1])  # data_source is at index 0
            all_group_types.add(result[2])  # column_base_type is at index 1

    OTHER_KEY = 'Other'
    all_group_types.add(OTHER_KEY)  # Add 'Other' that will be all types < x% percent
    all_data_sources = sorted(list(all_data_sources))
    all_group_types = sorted(list(all_group_types), key=get_group_type_order)
    all_usage_types = sorted(all_results.keys())
    print('Found group types:', all_group_types)
    print('Found data sources:', all_data_sources)
    print('Found usage types:', all_usage_types)

    n_usage_types = len(all_usage_types)

    fig, axes = plt.subplots(
        nrows=n_usage_types,
        ncols=1,
        figsize=(5, 1.3 * n_usage_types),  # wide, not too tall
        constrained_layout=True
    )

    # If only one subplot, make axes iterable
    if len(all_usage_types) == 1:
        axes = [axes]

    handles_all = []
    labels_all = []

    for index, (data_usage_type, ax) in enumerate(zip(all_usage_types, axes)):
        n_data_sources = len(all_data_sources)

        # layout: Key = Logical Type, value = [Val Datasource 1, Val DataSource 2, Val DataSource 3]
        percentages_per_type = {
            group_type: np.zeros(n_data_sources) for group_type in all_group_types
        }

        for (data_source_index, data_source) in enumerate(sorted(all_data_sources)):
            relevant_rows = [row for row in all_results[data_usage_type] if row[1] == data_source]

            percentage_sum = 0
            other_sum = 0
            for row in relevant_rows:
                group_type = row[2]
                percentage = row[percentage_index]
                if percentage < 0.03:  # less than 1%
                    other_sum += percentage
                else:
                    percentages_per_type[group_type][data_source_index] = percentage
                percentage_sum += percentage

            percentages_per_type[OTHER_KEY][data_source_index] = other_sum

            # assert abs(percentage_sum - 1.0) < 0.01, (
            #     f"Percentage sum for {data_source} in {data_usage_type} is not 100%: {percentage_sum}"
            # )
            print(f"Enable assert again!!! ")

        # kick percentages out where the max is below 0.03
        for group_type in list(percentages_per_type.keys()):
            if np.max(percentages_per_type[group_type]) < 0.03 and group_type != OTHER_KEY:
                del percentages_per_type[group_type]

        bar_height = 0.7  # smaller than 0.6 → slimmer boxes

        left = np.zeros(n_data_sources)

        ax.invert_yaxis()
        ax.xaxis.set_visible(False)

        ax.set_xlim(0, 1)

        for spine in ax.spines.values():
            spine.set_visible(False)

        for group_type, percentages in percentages_per_type.items():
            color, hatch = get_group_type_color_and_hatch(group_type)
            p = ax.barh(
                all_data_sources,  # y-axis labels are the data sources
                percentages,
                height=bar_height,
                label=group_type,
                left=left,
                color=color,
            )
            left += percentages
            format_percent = lambda x: f"{round(x * 100)}\%" if x > 0.015 else ""  # format for percentages
            ax.bar_label(p, label_type='center', fontsize=10, labels=[format_percent(x) for x in percentages],
                         color='white',
                         padding=0, weight='bold', rotation=0)

            # Remove default y-axis tick labels
            ax.set_yticks([])

            # Add custom labels inside the bars, aligned right from y-axis
            for i, source in enumerate(all_data_sources):
                ax.text(0.01, i, source, va='center', ha='left', color='white', fontsize=10)

        ax.set_title(f"{data_usage_type}", fontsize=12, fontweight='bold', color='black', pad=4)
        handles, labels = ax.get_legend_handles_labels()
        for h, l in zip(handles, labels):
            if l not in labels_all:  # prevent duplicates
                handles_all.append(h)
                labels_all.append(l)

    fig.suptitle('c', color='white', fontsize=15)  # hide the super title, but we need space for the legend

    fig.legend(handles_all, labels_all, loc='upper center', bbox_to_anchor=(0.5, 1.0),
               ncol=5, fontsize=10, borderaxespad=0., alignment='center')
    # save as pdf
    output_file = os.path.join(output_dir, f"{count_type}.{OUTPUT_FORMAT}")
    plt.savefig(output_file, format=OUTPUT_FORMAT, bbox_inches='tight', dpi=300)
    print(f"Saved plot to {output_file}")

    plt.show()


def column_semantic_type_syntactic_usage_plot(con: duckdb.DuckDBPyConnection):
    """
    Create a stacked bar plot showing column semantic types and their distribution by syntactic type.
    """

    # Dictionary to store all results
    all_results = get_results(con, "ifnull(semantic_type_syntactic, 'Other') as semantic_type_syntactic",
                              "(semantic_type_syntactic != 'Test' or semantic_type_syntactic is null) and column_base_type = 'Text'")

    # Create stacked bar plot for column counts
    dir = os.path.join(PLOTS_DIR, 'column_usage', 'semantic_type_syntactic')
    create_stacked_bar_plot(all_results, 'column_cnt', dir)
    # create_stacked_bar_plot(all_results, 'column_distinct_cnt', dir)
    # create_stacked_bar_plot(all_results, 'query_cnt', dir)
    # create_stacked_bar_plot(all_results, 'repo_cnt', dir)


def unify_llm_type(semantic_type: Optional[str]) -> str:
    # {None, '', 'Password', 'Identifier', 'Contact', 'Boolean', 'Numeric', 'URL', 'Location',
    # '', 'DateTime', 'Category', 'Email', 'Name', 'PhoneNumber', 'FullText', 'Title', 'Function'}

    # Gender, Color are transformed to 'Category'
    if semantic_type in ['Gender', 'Color']:
        return 'Category'

    # Email, PhoneNumber are transformed to 'Contact'
    if semantic_type in ['Email', 'PhoneNumber']:
        return 'Contact'

    return semantic_type if semantic_type else 'Unknown'


def column_semantic_type_llm_usage_plot(con: duckdb.DuckDBPyConnection, output_dir: str = PLOTS_DIR):
    """
    Create a stacked bar plot showing column semantic types and their distribution by LLM usage.
    """
    con.create_function('unify_llm_type', unify_llm_type, null_handling='SPECIAL')
    # Dictionary to store all results
    all_results = get_results(
        con, "unify_llm_type(semantic_type_llm)",
        "(group_column != 'Test' or group_column is null) and column_base_type = 'Text'",
        "JOIN (SELECT column_id as column_id_llm, semantic_type as semantic_type_llm FROM '/Users/paul/workspace/SqlPile/src/data_analysis/semantic_types.csv') AS st ON st.column_id_llm = c.id"
    )

    # Create stacked bar plot for column counts
    dir = os.path.join(output_dir, 'column_usage', 'physical_type')
    create_stacked_bar_plot(all_results, 'column_cnt', dir)
    # create_stacked_bar_plot(all_results, 'column_distinct_cnt', dir)
    # create_stacked_bar_plot(all_results, 'query_cnt', dir)
    # create_stacked_bar_plot(all_results, 'repo_cnt', dir)


OUTPUT_FORMAT = 'pdf'


def get_group_type_color_and_hatch(usage_type: str) -> tuple:
    """
    Get a color and hatch pattern for the given column type. DateTime, Flot, Int, Other
    """
    color_map = {
        'DateTime': '#E76F51',  # Warm terracotta
        'Float': '#2A9D8F',  # Teal green
        'Int': '#264653',  # Deep navy
        'Text': '#c94b71',  # Soft pink
        'Other': '#44509c',  # Soft orange

        'Unknown': '#495057',  # Dark neutral grey
        'FullText': '#c94b71',  # Dark orange
        'Identifier': '#6a4c93',  # Deep purple
        'Label': '#b8860b',  # Dark goldenrod
        'Category': '#1b7a6e',  # Dark teal
        'Name': '#14213d',  # Very dark navy
        'URL': '#b23a28',  # Dark terracotta red
        'Contact': '#7b2cbf',  # Rich violet
        'Location': '#006d77',  # Deep blue-green
        'Boolean': '#cc5803',  # Burnt orange
        'Numeric': '#155d27',  # Deep forest green
    }

    hatch_map = {
        'DateTime': '///',  # Diagonal lines
        'Float': '\\\\',  # Backward diagonal lines
        'Int': 'xx',  # Crosshatch
        'Text': '--',  # Dashes
        'Other': '',  # No hatch for Other
    }

    # if not in color map return a random color
    if usage_type not in color_map:
        print(f"Warning: Usage type '{usage_type}' not found in color map. Using random color.")
        import random
        # set the seed to the type to get a consistent color for the same type
        random.seed(usage_type)
        random_color = f'#{random.randint(0, 0xFFFFFF):06x}'  # Generate a random hex color
        return random_color, ''  # No hatch for random colors

    return color_map[usage_type], hatch_map.get(usage_type, '')  # Default to no hatch if not specified


def get_group_type_order(usage_type: str) -> int:
    """
    Get an order for the given column type. This is used to sort the column types in the plot.
    """
    order_map = {
        'DateTime': 2,
        'Float': 1,
        'Int': 0,
        'Text': 4,
        'Other': 3,
    }
    return order_map.get(usage_type, 5)  # Default to 5 for unknown types


if __name__ == "__main__":
    con = get_con(read_only=True)

    column_physical_type_usage_plot(con)
    # column_semantic_type_sato_usage_plot(con)
    column_semantic_type_llm_usage_plot(con)
    # column_semantic_type_syntactic_usage_plot(con)
