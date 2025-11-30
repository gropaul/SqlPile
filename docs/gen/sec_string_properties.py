import os
from typing import Optional, Literal

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from docs.gen.sec_semantic_type_results import MAX_VALUES_PER_EXAMPLE
from docs.gen.utils import get_multi_figure, format_number
from src.config import get_con, LATEX_ASSETS_DIR, LATEX_GEN_DIR, MAX_VALUES_TO_ANALYZE_PER_COLUMN
from src.data_analysis.semantic_type.models import BASE_SEMANTIC_TYPES

SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

SECTION_TEMPLATE = """
{STRING_DUPS_SEMANTIC_TYPE_FIGURE}
{STRING_DUPS_TPC_VS_OTHERS_FIGURE}

{STRING_PROPERTY_FIGURE}
"""

MIN_ROWS_FOR_PROPERTY_ANALYSIS = 10000

plt.rcParams.update({
    "text.usetex": True,  # use LaTeX for all text
    "font.family": "serif",  # set serif font
    "font.serif": ["Computer Modern Roman"],  # or another LaTeX font
})


def _build_query(metric_col: str, p_low: float = 0.001, p_high: float = 0.999) -> str:
    """
    Build a query that filters rows per semantic_type_llm to the [p_low, p_high] percentiles
    of the chosen metric column.
    """
    return f"""
        WITH tmp AS (
            SELECT 
              unify_llm_type(semantic_type_llm) as semantic_type_llm, 
              counts:  list_sort(list_transform(char_histogram, (x -> x.cnt)), 'DESC'), 
              total: list_sum(counts),
              indices: range(0, len(counts)),
              percentages: list_transform(counts, (x -> x/ total)),
              percentages2:  list_filter(counts, (x -> x / total > 0.01)),
              empty_or_null_rate: (count_null + count_empty) / count,
              *
            FROM column_stats_text AS stats
            JOIN "columns" ON columns.id = stats.column_id
            JOIN tables ON columns.table_id = tables.id
            WHERE 
                semantic_type_llm IS NOT NULL 
                AND semantic_type_llm NOT IN ('Other', 'Test')
                AND count >= {MIN_ROWS_FOR_PROPERTY_ANALYSIS}
        ),
        data as (
            SELECT 
              semantic_type_llm,
              percentages, 
              percentages_sum: list_transform(indices,  (x -> list_sum(percentages[0:x+1]))), 
              percentages_to_bound: list_filter(percentages_sum, (x -> x < 0.99)),
              count_filtered: len(percentages_to_bound),
              count_unfiltered: len(percentages),
              empty_or_null_rate,
              {metric_col} AS metric_value
            FROM tmp
        ),
        bounds AS (
            SELECT
                semantic_type_llm,
                percentile_cont({p_low})  WITHIN GROUP (ORDER BY metric_value) AS p_low,
                percentile_cont({p_high}) WITHIN GROUP (ORDER BY metric_value) AS p_high
            FROM data
            GROUP BY semantic_type_llm
        )
        SELECT d.semantic_type_llm, d.metric_value
        FROM data d
        JOIN bounds b USING (semantic_type_llm)
        WHERE d.metric_value BETWEEN b.p_low AND b.p_high
        ORDER BY d.semantic_type_llm
        """


def boxplot_by_type(
        con,
        metric_col: str = "stddev_length",
        *,
        p_low: float = 0.01,
        p_high: float = 0.99,
        log_scale: bool = True,
        ylabel: Optional[str] = None,
        figsize=(1.7 * 1.1, 2.5* 1.5),
        rotate_xticks: int = 90,
        path: str = '',
        percentage: bool = False,
):
    """Plot boxplots of the chosen metric per semantic type."""
    query = _build_query(metric_col=metric_col, p_low=p_low, p_high=p_high)
    df = con.execute(query).fetchdf()

    plt.figure(figsize=figsize)
    sns.boxplot(
        data=df,
        x="semantic_type_llm",
        y="metric_value",
        hue="semantic_type_llm",
        order=[x.name for x in BASE_SEMANTIC_TYPES if x.name not in ('Other', 'Test')],
        palette="Set3",
        showfliers=False,
        legend=False,  # suppress duplicate legend
    )

    if log_scale:
        plt.yscale("log")

    # make the font bigger
    plt.ylabel(ylabel)
    plt.xticks(rotation=rotate_xticks)
    plt.tight_layout()

    if percentage:
        if log_scale:
            plt.yticks([0.0001, 0.001, 0.01, 0.1, 1.0], ['0.01\\%', '0.1\\%', '1\\%', '10\\%', '100\\%'])
        else:
            plt.yticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0], ['0\\%', '20\\%', '40\\%', '60\\%', '80\\%', '100\\%'])

    # no x label
    plt.xlabel("")

    if path:
        # if the path is a dir, we append a filename
        if os.path.isdir(path):
            path = os.path.join(path, f"{metric_col}.pdf")
        plt.savefig(path, format='pdf', bbox_inches='tight', dpi=300)
    else:
        plt.show()

    plt.clf()


def get_string_prop_plots(con, path: str):
    boxplot_by_type(con, "count_filtered", path=path, ylabel="Number of distinct characters (filtered)",
                    log_scale=False)
    boxplot_by_type(con, "count_unfiltered", path=path, ylabel="Number of distinct characters", log_scale=False)
    boxplot_by_type(con, "avg_length", path=path, ylabel=None)
    boxplot_by_type(con, "stddev_length", path=path, ylabel=None)
    boxplot_by_type(con, "repeat_rate", path=path, ylabel="Duplicates per Value", log_scale=True)
    boxplot_by_type(con, "empty_or_null_rate", path=path, ylabel=None, log_scale=False, percentage=False)

Mode = Literal['PerSemanticType', 'TpcHVsOthers']


def generate_dups_plot(con, mode: Mode, skip_null_or_empty: bool = False, ):
    repo_group_clause = ""
    group_clause = ""

    if mode == 'TpcHVsOthers':
        repo_group_clause = "if(repos.repo_url LIKE '%tpc-h%' OR repos.repo_url LIKE '%tpc-ds%', 'TPC', 'Other') as repo_group"
        group_clause = "'All' as semantic_type_llm"
    elif mode == 'PerSemanticType':
        repo_group_clause = "if(repos.repo_url LIKE '%tpc-h%' OR repos.repo_url LIKE '%tpc-ds%', 'TPC', 'Other') as repo_group"

        group_clause = "semantic_type_llm"

    histograms = con.sql(f"""
        SELECT 
            {repo_group_clause},
            {group_clause},
            list({{
                count_distinct: count_distinct,
                count: count,
                count_empty: ifnull(count_empty, 0),
                count_null: ifnull(count_null, 0),
                value_histogram: value_histogram
            }}), 
        FROM column_stats_text AS stats
        JOIN columns ON columns.id = stats.column_id
        JOIN tables ON tables.id = columns.table_id
        JOIN repos ON repos.id = tables.repo_id
        WHERE 
            semantic_type_llm IS NOT NULL 
            AND semantic_type_llm not in ('Other', 'Test') 
            AND stats.count = {MAX_VALUES_TO_ANALYZE_PER_COLUMN}
        GROUP BY ALL
    """).fetchall()

    all_logged = []

    # First pass: compute log data range
    for (repo_group, semantic_type_llm, data_list) in histograms:
        # plot the values on a log scale
        plt.figure(figsize=(1.7, 2.5))

        top_3_percentage = []
        top_10_percentage = []
        top_100_percentage = []
        all = []

        for data in data_list:

            hist = data['value_histogram']



            percentage_sum_3 = 0
            percentage_sum_10 = 0
            percentage_sum_100 = 0

            values_skipped = 0
            values_looked_at = 0
            for (i, entry) in enumerate(hist):
                cnt = entry['cnt']
                value = entry['value']
                if (value is None or value == '') and skip_null_or_empty:
                    values_skipped += 1
                    continue

                percent = cnt
                if values_looked_at < 3:
                    percentage_sum_3 += percent
                if values_looked_at < 10:
                    percentage_sum_10 += percent
                if values_looked_at < 100:
                    percentage_sum_100 += percent
                values_looked_at += 1


            n_values = values_looked_at
            top_3_percentage.append(percentage_sum_3)
            top_10_percentage.append(percentage_sum_10 / min(10, n_values))
            top_100_percentage.append(percentage_sum_100 / min(100, n_values))

            all_count =  data['count'] - data['count_null'] - data['count_empty']
            has_emptys = data['count_empty'] > 0
            all_distinct = data['count_distinct'] - (1 if has_emptys else 0)

            if all_distinct == 0:
                print(f"Warning: all_distinct is 0 for {semantic_type_llm} in {repo_group}, skipping.")
            all.append(all_count / all_distinct)

            if semantic_type_llm == 'Category':
                print(hist)
                # print the last of top_10_percentage, top_100_percentage, all
                print(f"Category Dups {repo_group} - Top 3: {top_3_percentage[-1]:.2f}, "
                        f"Top 10: {top_10_percentage[-1]:.2f}, "
                        f"Top 100: {top_100_percentage[-1]:.2f}, "
                        f"All: {all[-1]:.2f}")
                print()

        # make 3 boxplots in one figure
        data = [top_3_percentage, top_10_percentage, top_100_percentage, all]

        # there is an error if top_10_percentage < all
        for (top_10_val, all_val) in zip(top_10_percentage, all):
            if top_10_val < all_val:
                print(f"Error: top 10 dups {top_10_val} is less than all {all_val} for {semantic_type_llm} in {repo_group}")

        plt.boxplot(data, tick_labels=['Top 3  ', 'Top 10 ', 'Top 100', 'All'], showfliers=False, showmeans=False)
        plt.ylabel('%')
        plt.yscale('log')
        # nice y ticks
        plt.yticks([1, 10, 100, 1000, MAX_VALUES_TO_ANALYZE_PER_COLUMN / 10, MAX_VALUES_TO_ANALYZE_PER_COLUMN], ['1', '10', '100', '1k', '12k', '122k'])

        plt.ylim(0.9, MAX_VALUES_TO_ANALYZE_PER_COLUMN * 2)
        plt.tight_layout()

        # rotate x labels by 45 degrees
        plt.xticks(rotation=90)
        # make font bigger

        # save to latex assets dir
        path = os.path.join(LATEX_ASSETS_DIR, 'string_stats', 'duplicates', repo_group)
        os.makedirs(path, exist_ok=True)
        plt.savefig(os.path.join(path, f'{semantic_type_llm}_duplicates.pdf'), format='pdf', bbox_inches='tight',
                    dpi=300)


def generate_section(con):
    path = os.path.join(LATEX_ASSETS_DIR, 'string_stats')
    os.makedirs(path, exist_ok=True)
    local_assets_dir = 'assets/string_stats'

    generate_dups_plot(con, mode='PerSemanticType', skip_null_or_empty=True)
    generate_dups_plot(con, mode='TpcHVsOthers', skip_null_or_empty=True)

    others_dir = os.path.join(local_assets_dir, 'duplicates', 'Other')

    figure_dups_semantic_type = get_multi_figure(
        label="img:string_properties_dups",
        paths=[
            os.path.join(others_dir, 'Category_duplicates.pdf'),
            os.path.join(others_dir, 'Entity_duplicates.pdf'),
            os.path.join(others_dir, 'FullText_duplicates.pdf'),
            os.path.join(others_dir, 'Identifier_duplicates.pdf'),
            os.path.join(others_dir, 'Numeric_duplicates.pdf'),
            os.path.join(others_dir, 'Structured_duplicates.pdf'),
        ],
        caption=f"Duplicates per row group ({format_number(MAX_VALUES_TO_ANALYZE_PER_COLUMN)} values) per semantic types for all columns excluding TPC-[H/DS] datasets. Only strings that are not null or empty are considered.",
        captions=[
            "Category",
            "Entity",
            "FullText",
            "Identifier",
            "Numeric",
            "Structured",
        ]
    )

    figure_dups_tpc_vs_others = get_multi_figure(
        two_column=False,
        main_percentage=0.7,
        label="img:string_properties_dups_tpc_vs_others",
        paths=[
            os.path.join(local_assets_dir, 'duplicates', 'TPC', 'All_duplicates.pdf'),
            os.path.join(local_assets_dir, 'duplicates', 'Other', 'All_duplicates.pdf'),
        ],
        caption="Duplicate value rates for TPC-[H/DS] datasets vs. all other datasets.",
        captions=[
            "TPC-[H/DS]",
            "Other",
        ]
    )

    get_string_prop_plots(con, path=path)

    n_columns = con.execute(f"SELECT COUNT(*) FROM column_stats_text WHERE count >= {MIN_ROWS_FOR_PROPERTY_ANALYSIS}").fetchone()[0]
        

    multi_figure = get_multi_figure(
        two_column=False,
        label="img:string_properties",
        paths=[
            os.path.join(local_assets_dir, 'avg_length.pdf'),
            os.path.join(local_assets_dir, 'stddev_length.pdf'),
            # os.path.join(local_assets_dir, 'count_filtered.pdf'),
            # os.path.join(local_assets_dir, 'count_unfiltered.pdf'),
            os.path.join(local_assets_dir, 'empty_or_null_rate.pdf'),
            # os.path.join(local_assets_dir, 'repeat_rate.pdf'),
        ],
        captions=[
            "Avg. length",
            "Std. dev. length",
            # "Distinct chars (filtered*)",
            # "Distinct chars",
            "Null or empty \%",
            # "Duplicates",
        ],

        caption=f"String properties per semantic type for columns with at least {format_number(MIN_ROWS_FOR_PROPERTY_ANALYSIS)} rows (n={n_columns}). The filtered distinct chars only counts the number of chars that make up 99\\% of all chars.",
    )

    section = SECTION_TEMPLATE.format(
        STRING_PROPERTY_FIGURE=multi_figure,
        STRING_DUPS_SEMANTIC_TYPE_FIGURE=figure_dups_semantic_type,
        STRING_DUPS_TPC_VS_OTHERS_FIGURE=figure_dups_tpc_vs_others,
    )

    with open(os.path.join(LATEX_GEN_DIR, SECTION_NAME), "w") as f:
        f.write(section)


if __name__ == "__main__":
    con = get_con()
    generate_section(con)
