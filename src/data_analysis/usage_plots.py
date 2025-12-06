import colorsys
import os
import os.path
from typing import List, Optional, Dict

import duckdb
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm

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
    ['Aggregate', 'Group Key', 'Join Key', 'Payload Column', 'Projection', 'Filter', 'Scan', 'Order Key',
     'Window Function']
    snowflake_data = {
        'All': {
            'Int': -1.0,
            'Float': -1.0,
            'Text': -1.0,
            'DateTime': -1.0
        },
        'Aggregate': {
            'Int': 0.49,
            'Float': 0.067,
            'Text': 0.34,
            'DateTime': 0.085
        },
        'Group Key': {
            'Int': -1.0,
            'Float': -1.0,
            'Text': -1.0,
            'DateTime': -1.0
        },
        'Join Key': {
            'Int': 0.39,
            'Float': 0.02,
            'Text': 0.46,
            'DateTime': 0.11
        },
        'Payload Column': {
            'Int': -1.0,
            'Float': -1.0,
            'Text': -1.0,
            'DateTime': -1.0
        },
        'Projection': {
            'Int': -1.0,
            'Float': -1.0,
            'Text': -1.0,
            'DateTime': -1.0
        },
        'Filter': {
            'Int': 0.25,
            'Float': 0.02,
            'Text': 0.58,
            'DateTime': 0.125
        },
        'Scan': {
            'Int': -1.0,
            'Float': -1.0,
            'Text': -1.0,
            'DateTime': -1.0
        },
        'Order Key': {
            'Int': 0.39,
            'Float': 0.01,
            'Text': 0.38,
            'DateTime': 0.20
        },
        'Window Function': {
            'Int': -1.0,
            'Float': -1.0,
            'Text': -1.0,
            'DateTime': -1.0
        }
    }

    create_vertical_bar_plot(all_results, 'column_cnt', output_dir,
                             all_groups_ordered=['Text'],
                             extra_data=snowflake_data, extra_label='Snowflake', needs_legend=False)
    # create_stacked_bar_plot(all_results, 'column_distinct_cnt', dir)
    # create_stacked_bar_plot(all_results, 'query_cnt', dir)
    # create_stacked_bar_plot(all_results, 'repo_cnt', dir)


def get_usage_types(con: duckdb.DuckDBPyConnection) -> List[str]:
    usage_types = con.execute("""SELECT DISTINCT unifiy_usage_types(usage_type)
                                 FROM column_usages
                                 ORDER BY usage_type""").fetchall()
    usage_types = [usage[0] for usage in usage_types]
    print(f"Found {len(usage_types)} usage types: {usage_types}")
    print("Only returnign one usage type for testing")

    # return usage_types[:1]  #
    return usage_types


def get_data_for_usage(con: duckdb.DuckDBPyConnection, group_column: str, where_clause: str = "TRUE ",
                       join_clause: str = "", usage: Optional[str] = None) -> str:
    query = f"""
                WITH unnested_ids AS (
                    SELECT id, query_id, unnest(column_ids) AS column_id, unifiy_usage_types(usage_type) as usage_type, expression, meta_data
                    FROM column_usages
                    -- USING SAMPLE 5%
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
                    AND len(list_distinct(list_transform(history[:-2], x -> x.expression_class))) <= 1 -- this only takes usages that come straight from the column, not through transformations
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

    print('Generating outlier list...')

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

    for usage in tqdm(usage_types, desc="Getting data for usage types"):
        usage_result = get_data_for_usage(con, group_column, where_clause, join_clause, usage)
        all_results[usage] = usage_result

    # create one result for all usage types
    all_results['All'] = get_data_for_usage(con, group_column, where_clause, join_clause, None)

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


def hsv_to_hex(h: float, s: float, v: float) -> str:
    r, g, b = colorsys.hsv_to_rgb(h, s, v)
    return "#{:02X}{:02X}{:02X}".format(int(round(r * 255)),
                                        int(round(g * 255)),
                                        int(round(b * 255)))


def generate_colors(n: int, brightness: float = 0.8, saturation: float = 0.9):
    """
    n: number of colors
    brightness: HSV 'value' in [0,1] (same for all colors)
    saturation: HSV 'saturation' in [0,1] (same for all colors)
    """
    if n <= 0:
        return []
    # Equally space hues on [0,1)
    step = 1.0 / n
    return [hsv_to_hex(i * step, saturation, brightness) for i in range(n)]


def generate_colors_from_cmap(n: int, cmap_name: str = "Set2"):
    """
    Generate `n` colors from a given qualitative colormap (like Dark2).
    """
    cmap = plt.get_cmap(cmap_name)
    return [cmap(i) for i in range(n)]


def create_stacked_bar_plot(all_results, count_type, output_dir, all_usage_types_ordered: List[str] = None):
    """
    Create a stacked bar plot for the given count type, showing percentages.

    Args:
        all_results: Dictionary with usage_type as key:
            {
                'Filter': [
                    ('Filter', 'DBPile', 'Text', 0.5, 0.4),
                    ('Filter', 'TPC-H', 'Int', 0.3, 0.4 ),
                    ...
                ],
                'Join': [
                    ('Join', 'DBPile', 'Text', 0.6, 0.5),
                    ('Join', 'TPC-H', 'Int', 0.4, 0.5 ),
                    ...
                ],
            }
        count_type: The type of count to plot ('column_cnt', 'query_cnt', or 'repo_cnt')
        output_dir: Directory to save the plot
    """

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Map count_type to index in the result tuple
    count_indices = {
        'column_cnt': 3,  # Index of column_cnt in the result tuple
        'column_distinct_cnt': 4,  # Index of column_cnt in the result tuple
        'query_cnt': 5,  # Index of query_cnt in the result tuple
        'repo_cnt': 6,  # Index of repo_cnt in the result tuple
        'column_base_type': 3,  # Index of column_cnt in the result tuple
        'semantic_type_llm': 3  # Index of column_cnt in the result tuple
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
    all_group_types = sorted(list(all_group_types), key=get_order_key)
    if all_usage_types_ordered:
        all_usage_types = all_usage_types_ordered
    else:
        all_usage_types = sorted(all_results.keys())
    print('Found group types:', all_group_types)
    print('Found data sources:', all_data_sources)
    print('Found usage types:', all_usage_types)

    n_usage_types = len(all_usage_types)

    height_per_plot = (1 / 3) * len(all_data_sources)
    fig, axes = plt.subplots(
        nrows=n_usage_types,
        ncols=1,
        figsize=(5, height_per_plot * n_usage_types),  # wide, not too tall
        constrained_layout=True
    )

    # If only one subplot, make axes iterable
    if len(all_usage_types) == 1:
        axes = [axes]

    handles_all = []
    labels_all = []

    all_group_types_filtered = []

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
                if percentage is None:
                    percentage = 0.0
                if percentage < 0.03:  # less than 1%
                    other_sum += percentage
                else:
                    percentages_per_type[group_type][data_source_index] = percentage
                percentage_sum += percentage

            # there might be already some other sum, so we can't assign but have to add
            existing_sum = percentages_per_type.get(OTHER_KEY, np.zeros(n_data_sources))[data_source_index]
            percentages_per_type[OTHER_KEY][data_source_index] = existing_sum + other_sum

            # assert abs(percentage_sum - 1.0) < 0.01, (
            #     f"Percentage sum for {data_source} in {data_usage_type} is not 100%: {percentage_sum}"
            # )
            if abs(percentage_sum - 1.0) >= 0.01:
                print(f"Warning: Percentage sum for {data_source} in {data_usage_type} is not 100%: {percentage_sum}")
        print(f"Enable assert again!!! ")

        # kick percentages out where the max is below 0.03
        for group_type in list(percentages_per_type.keys()):
            if np.max(percentages_per_type[group_type]) < 0.03 and group_type != OTHER_KEY:
                del percentages_per_type[group_type]
            else:
                if group_type not in all_group_types_filtered:
                    all_group_types_filtered.append(group_type)

        bar_height = 0.8

        lefts = np.zeros(n_data_sources)

        ax.invert_yaxis()
        ax.xaxis.set_visible(False)

        ax.set_xlim(0, 1)

        for spine in ax.spines.values():
            spine.set_visible(False)

        for group_type, percentages in percentages_per_type.items():
            color, hatch = get_group_type_color_and_hatch(group_type, all_group_types_filtered)
            # make slightly lighter color for better visibility
            lighter_color = tuple(min(1.0, c + 0.05) for c in color[:3]) if isinstance(color, tuple) else color

            p = ax.barh(
                all_data_sources,  # y-axis labels are the data sources
                percentages,
                height=bar_height,
                label=group_type,
                left=lefts,
                color=lighter_color,
                hatch=hatch,
                edgecolor=color

            )

            format_percent = lambda percentage, left: f"{round(percentage * 100)}\%" if percentage > 0.015 and left + (
                    percentage / 2) > 0.15 else ""  # format for percentages
            labels = [format_percent(percentage, left) for percentage, left in zip(percentages, lefts)]
            ax.bar_label(p, label_type='center', fontsize=10, labels=labels,
                         padding=0, rotation=0)

            # Remove default y-axis tick labels
            ax.set_yticks([])
            lefts += percentages

        # Add custom labels inside the bars, aligned right from y-axis
        for i, source in enumerate(all_data_sources):
            ax.text(0.01, i, source, va='center', ha='left', color='black', weight='normal', fontsize=10)

        ax.set_title(f"{data_usage_type}", fontsize=12, color='black', pad=4)
        handles, labels = ax.get_legend_handles_labels()
        for h, l in zip(handles, labels):
            if l not in labels_all:  # prevent duplicates
                handles_all.append(h)
                labels_all.append(l)

    n_cols = 5 if len(handles_all) == 5 else 4
    n_rows = (len(handles_all) + n_cols - 1) // n_cols
    title_size = 15 * n_rows

    fig.suptitle('c', color='white', fontsize=title_size)  # hide the super title, but we need space for the legend

    fig.legend(handles_all, labels_all, loc='upper center', bbox_to_anchor=(0.5, 1.0),
               ncol=n_cols, fontsize=10, borderaxespad=0., alignment='center', frameon=False)

    # save as pdf
    output_file = os.path.join(output_dir, f"{count_type}.{OUTPUT_FORMAT}")
    plt.savefig(output_file, format=OUTPUT_FORMAT, bbox_inches='tight', dpi=300)
    print(f"Saved plot to {output_file}")

    plt.show()


def get_hatch_for_source(data_source: str):
    hatch_map = {
        'DBPile': '',
        'SQLStorm': '///',
        'TPC': '\\\\\\',
        'Snowflake': 'xxx',
        'Other': '...'
    }
    if data_source in hatch_map:
        return hatch_map[data_source]
    else:
        return None

def get_color_for_source(data_source: str):
    color_map = {
        'DBPile': (0.4, 0.7608, 0.6471, 1.0),
        'SQLStorm': (0.9882, 0.5529, 0.3843, 1.0),
        'TPC': (0.5529, 0.6275, 0.7961, 1.0),
        'Snowflake': (0.6510, 0.8471, 0.3294, 1.0),
        'Other': (0.5804, 0.4039, 0.7412, 1.0)
    }
    if data_source in color_map:
        return color_map[data_source]
    else:
        return None


def get_lighter_color(color):
    lighter_color = tuple(min(1.0, c + 0.05) for c in color[:3]) if isinstance(color, tuple) else color
    return lighter_color


def create_vertical_bar_plot(all_results, count_type, output_dir, all_usage_types_ordered: List[str] = None,
                             all_groups_ordered: List[str] = None, extra_data: Dict = None, extra_label: str = "",
                             needs_y_ticks: bool = True, needs_legend: bool = True, custom_legend: List[str] = None):
    """
    Create individual vertical grouped bar plots for each usage_type, showing percentages.
    Each usage_type gets its own separate plot file with vertical bars grouped by group_type,
    with one bar per data_source within each group.

    Args:
        all_results: Dictionary with usage_type as key
        count_type: The type of count to plot
        output_dir: Directory to save the plot
        all_usage_types_ordered: Optional ordered list of usage types
        all_groups_ordered: Optional ordered list of group types
    """

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Map count_type to index in the result tuple
    count_indices = {
        'column_cnt': 3,
        'column_distinct_cnt': 4,
        'query_cnt': 5,
        'repo_cnt': 6,
        'column_base_type': 3,
        'semantic_type_llm': 3
    }

    percentage_index = count_indices[count_type]

    # Collect all unique column_base_types across all usage types
    all_group_types = set()
    all_data_sources = set()
    for results in all_results.values():
        for result in results:
            all_data_sources.add(result[1])  # data_source
            all_group_types.add(result[2])  # group_type

    OTHER_KEY = 'Other'
    all_group_types.add(OTHER_KEY)
    all_data_sources = sorted(list(all_data_sources))
    if all_usage_types_ordered:
        all_usage_types = all_usage_types_ordered
    else:
        all_usage_types = sorted(all_results.keys())

    if all_groups_ordered:
        all_group_types = all_groups_ordered
    else:
        all_group_types = sorted(list(all_group_types), key=get_order_key)

    print('Found group types:', all_group_types)
    print('Found data sources:', all_data_sources)
    print('Found usage types:', all_usage_types)

    # Create color map for data sources
    colors = plt.cm.Set2(np.linspace(0, 1, len(all_data_sources)))
    data_source_colors = {ds: colors[i] for i, ds in enumerate(all_data_sources)}

    all_group_types_filtered = []

    # Create a separate plot for each usage_type
    for data_usage_type in all_usage_types:

        # Collect data for this usage type
        percentages_per_type = {
            group_type: {ds: 0.0 for ds in all_data_sources}
            for group_type in all_group_types
        }

        for row in all_results[data_usage_type]:
            data_source = row[1]
            group_type = row[2]
            percentage = row[percentage_index]
            if percentage is None:
                percentage = 0.0

            if percentage < 0.03 and not all_groups_ordered:
                percentages_per_type[OTHER_KEY][data_source] += percentage
            else:
                if all_groups_ordered:
                    if group_type in all_group_types:
                        percentages_per_type[group_type][data_source] = percentage
                    else:
                        print(f"Warning: Group type {group_type} not in ordered list, skipping.")
                else:
                    percentages_per_type[group_type][data_source] = percentage
        # Filter out group types with max < 3%
        filtered_percentages = {}
        for group_type, ds_values in percentages_per_type.items():
            max_val = max(ds_values.values())
            if max_val >= 0.05 or all_groups_ordered is not None:
                filtered_percentages[group_type] = ds_values
                if group_type not in all_group_types_filtered:
                    all_group_types_filtered.append(group_type)

        if extra_data:
            if extra_label not in all_data_sources:
                all_data_sources.append(extra_label)
            if extra_data[data_usage_type]:
                extra_data_usage = extra_data[data_usage_type]
                for group_type, percentage in extra_data_usage.items():
                    if group_type not in filtered_percentages:
                        filtered_percentages[group_type] = {ds: 0.0 for ds in all_data_sources}
                    filtered_percentages[group_type][extra_label] = percentage
            # else:
            #     # add unknown with -1 for all data sources
            #     for group_type in extra_data[data_usage_type].keys():
            #         if group_type not in filtered_percentages:
            #             filtered_percentages[group_type] = {ds: 0.0 for ds in all_data_sources}
            #         filtered_percentages[group_type][extra_label] = -1.0

        # Prepare data for grouped bar chart
        group_types_to_plot = [gt for gt in all_group_types if gt in filtered_percentages]
        group_types_to_plot = sorted(group_types_to_plot, key=get_order_key)
        all_data_sources = sorted(all_data_sources, key=get_order_key)

        n_groups = len(group_types_to_plot)
        bar_width = 0.8 / len(all_data_sources)  # Total width per group is 0.8
        x = np.arange(n_groups)  # Group positions

        # 0.7 + 0.55 = 1.25
        # 0.7 + 6 * 0.55 = 4.0
        # 1.25 / 5.25 = 0.238
        # 4.0 / 5.25 = 0.761
        fig, ax = plt.subplots(figsize=(0.70 + 0.55 * len(all_group_types), 3.0))

        # Plot bars for each data source
        handles = []
        labels = []
        for ds_idx, data_source in enumerate(all_data_sources):
            values = [filtered_percentages[gt][data_source] for gt in group_types_to_plot]
            offset = (ds_idx - len(all_data_sources) / 2 + 0.5) * bar_width

            color = get_color_for_source(data_source)
            lighter_color = get_lighter_color(color)
            hatch = get_hatch_for_source(data_source)

            bars = ax.bar(
                x + offset,
                values,
                bar_width,
                label=data_source,
                color=lighter_color,
                edgecolor=color,
                hatch=hatch
            )

            handles.append(bars)
            labels.append(data_source)

            # Add percentage labels on bars
            for bar, val in zip(bars, values):
                height = bar.get_height()

                if 0 <= val <= 0.92:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        height + 0.01,
                        f'{int(val * 100)}\%',
                        ha='center',
                        va='bottom',
                        fontsize=8,
                        rotation=90
                    )
                if val < 0:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        0.01,
                        'N/A',
                        ha='center',
                        va='bottom',
                        fontsize=8,
                        rotation=90
                    )

        # Customize the plot
        # make percentage on y-axis ticks
        ax.set_xticks(x)

        if 'DateTime' in group_types_to_plot:
            group_types_to_plot[group_types_to_plot.index('DateTime')] = 'Date'
        ax.set_xticklabels(group_types_to_plot, rotation=90, ha='center', fontsize=10)
        ax.set_ylim(0, 1.0)
        if needs_y_ticks:
            ax.set_yticks(np.linspace(0, 1.0, 6), labels=[f'{int(y * 100)}\%' for y in np.linspace(0, 1.0, 6)],
                          fontsize=8)
        else:
            ax.set_yticks(np.linspace(0, 1.0, 6), labels=[f'' for y in np.linspace(0, 1.0, 6)],
                          fontsize=8)
        ax.grid(axis='y', alpha=0.3, linestyle='--')

        if needs_legend:

            if custom_legend:
                legend_colors = [get_color_for_source(label) for label in custom_legend]
                legend_hatch = [get_hatch_for_source(label) for label in custom_legend]
                legend_labels = custom_legend
                import matplotlib.patches as mpatches

                # Create legend handles
                handles = [mpatches.Patch(facecolor=get_lighter_color(color), label=label, hatch=hatch, edgecolor=color)
                           for color, label, hatch in zip(legend_colors, legend_labels, legend_hatch)]

                ax.legend(handles=handles, loc='upper center',
                          bbox_to_anchor=(0.5, 1.18), ncol=len(legend_labels),
                          fontsize=8, frameon=False)
            else:
                ax.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 1.18),
                          ncol=len(all_data_sources) // 2,
                          fontsize=8, frameon=False)

        # Define absolute margins in inches
        bottom_margin_inches = 0.8  # space for x-labels
        top_margin_inches = 0.5  # space for legend
        left_margin_inches = 0.4  if needs_y_ticks else 0.05  # left padding
        right_margin_inches = 0.05  # right padding

        # Get figure size
        fig_width, fig_height = fig.get_size_inches()

        # Convert to fractions
        bottom = bottom_margin_inches / fig_height
        top = 1 - (top_margin_inches / fig_height)
        left = left_margin_inches / fig_width
        right = 1 - (right_margin_inches / fig_width)

        fig.subplots_adjust(bottom=bottom, top=top, left=left, right=right)

        # Save without bbox_inches='tight'
        output_file = os.path.join(output_dir, f"{count_type}_{data_usage_type}.{OUTPUT_FORMAT}")
        # replace spaces with underscores and make lowercase
        output_file = output_file.replace(' ', '_').lower()
        plt.savefig(output_file, format=OUTPUT_FORMAT, dpi=300)
        plt.close(fig)  # Close to free memory

    print(f"Created {len(all_usage_types)} individual plots")


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


print('Warning: Structured, Boolean and Numeric, Structured, Url are transformed to Other. Mention this in the text!')


def column_semantic_type_llm_usage_plot(con: duckdb.DuckDBPyConnection, output_dir: str = PLOTS_DIR):
    """
    Create a stacked bar plot showing column semantic types and their distribution by LLM usage.
    """
    # Dictionary to store all results
    all_results = get_results(
        con, "semantic_type_llm",
        "group_column NOT IN  ('Test', 'Other') and group_column is not null and column_base_type = 'Text'"
    )

    # Create stacked bar plot for column counts
    create_vertical_bar_plot(all_results, 'column_cnt', output_dir,
                             all_groups_ordered=['Identifier', 'Category', 'Entity', 'FullText', 'Structured',
                                                 'Numeric'], needs_y_ticks=False, custom_legend=['DBPile', 'Snowflake',
                                                                                                   'SQLStorm', 'TPC'])
    # create_stacked_bar_plot(all_results, 'column_distinct_cnt', dir)
    # create_stacked_bar_plot(all_results, 'query_cnt', dir)
    # create_stacked_bar_plot(all_results, 'repo_cnt', dir)


OUTPUT_FORMAT = 'pdf'
HATCHES = ['//', '....', '**', '\\\\', 'xx', '++', 'oo', '**', '|||', '---']


def get_group_type_color_and_hatch(usage_type: str, all_types: List[str] = None):
    n = len(all_types) if all_types else 10

    index = all_types.index(usage_type) if all_types and usage_type in all_types else 0
    # colors = generate_colors(n, brightness=0.6, saturation=0.8)
    colors = generate_colors_from_cmap(8)

    hatch = HATCHES[index % len(HATCHES)]
    color = colors[index % len(colors)]
    return color, hatch


def get_order_key(usage_type: str) -> int:
    """
    Get an order for the given column type. This is used to sort the column types in the plot.
    """
    order_map = {
        # Logic Types
        'Int': 0,
        'Text': 1,
        'Float': 2,
        'DateTime': 3,
        'Other': 4,

        # Semantic Types
        'Name': 0,
        'Location': 1,
        'Identifier': 4,
        'FullText': 5,
        'Category': 6,
        'Contact': 7,

        # Soruce Types
        'DBPile': 0,
        'Snowflake': 1,
        'SQLStorm': 2,
        'TPC': 3,
    }

    return order_map.get(usage_type, 99)  # Default to 5 for unknown types


if __name__ == "__main__":
    con = get_con(read_only=True)

    column_physical_type_usage_plot(con)
    # column_semantic_type_sato_usage_plot(con)
    column_semantic_type_llm_usage_plot(con)
    # column_semantic_type_syntactic_usage_plot(con)
