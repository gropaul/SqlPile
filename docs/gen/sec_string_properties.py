import os

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from docs.gen.sec_semantic_type_results import MAX_VALUES_PER_EXAMPLE
from docs.gen.utils import get_multi_figure
from src.config import get_con, LATEX_ASSETS_DIR, LATEX_GEN_DIR, MAX_VALUES_TO_ANALYZE_PER_COLUMN
from src.data_analysis.semantic_type.models import SemanticType, BASE_SEMANTIC_TYPES

SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

plt.rcParams.update({
    "text.usetex": True,  # use LaTeX for all text
    "font.family": "serif",  # set serif font
    "font.serif": ["Computer Modern Roman"],  # or another LaTeX font
})

def _build_query(metric_col: str, p_low: float = 0.05, p_high: float = 0.95) -> str:
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
              empty_or_null_rate: count_null + count_empty / count,
              *
            FROM column_stats_text AS stats
            JOIN "columns" ON columns.id = stats.column_id
            JOIN tables ON columns.table_id = tables.id
            WHERE semantic_type_llm IS NOT NULL
        ),
        data as (
            SELECT 
              semantic_type_llm,
              percentages, 
              percentages_sum: list_transform(indices,  (x -> list_sum(percentages[0:x+1]))), 
              percentages_to_bound: list_filter(percentages_sum, (x -> x < 0.95)),
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
        p_low: float = 0.05,
        p_high: float = 0.95,
        log_scale: bool = True,
        ylabel: str | None = None,
        figsize=(2.3, 4),
        rotate_xticks: int = 90,
        path: str = ''
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
        order=[x.name for x in BASE_SEMANTIC_TYPES],
        palette="Set3",
        showfliers=False,
        legend=False,  # suppress duplicate legend
    )

    if log_scale:
        plt.yscale("log")
    if ylabel is None:
        ylabel = f"{metric_col}" + (" (log scale)" if log_scale else "")

    # make the font bigger
    plt.ylabel(ylabel)
    plt.xticks(rotation=rotate_xticks)
    plt.tight_layout()

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
    boxplot_by_type(con, "avg_length", path=path, ylabel="Average Length")
    boxplot_by_type(con, "stddev_length", path=path, ylabel="Standard Deviation of Length")
    boxplot_by_type(con, "repeat_rate", path=path, ylabel="Duplicates per Value", log_scale=True)
    boxplot_by_type(con, "empty_or_null_rate", path=path, ylabel="% Null Values", log_scale=False)


SECTION_TEMPLATE = """
{STRING_PROPERTY_FIGURE}
"""

def generate_dups_plot(con):
    from scipy.stats import gaussian_kde
    import numpy as np
    import matplotlib.pyplot as plt

    histograms = con.sql(f"""
        SELECT 
            if(repos.repo_url LIKE '%tpc-h%' OR repos.repo_url LIKE '%tpc-ds%', 'TPC', 'Other') as repo_group,
            -- 'Other' as repo_group,
            semantic_type_llm, 
            list(count_distinct), 
            list(value_histogram)
        FROM column_stats_text AS stats
        JOIN columns ON columns.id = stats.column_id
        JOIN tables ON tables.id = columns.table_id
        JOIN repos ON repos.id = tables.repo_id
        WHERE 
            semantic_type_llm IS NOT NULL 
            AND semantic_type_llm not in ('Other', 'Test') 
            -- and semantic_type_llm = 'Identifier'
            AND stats.count = {MAX_VALUES_TO_ANALYZE_PER_COLUMN}
        GROUP BY ALL
    """).fetchall()


    all_logged = []

    # First pass: compute log data range
    for (repo_group, semantic_type_llm, distincts, histograms_list,) in histograms:
        # plot the values on a log scale
        plt.figure(figsize=(2.5, 4))

        top_1_percentage = []
        top_10_percentage = []
        top_100_percentage = []
        all = []
        for distinct in distincts:
            all.append(MAX_VALUES_TO_ANALYZE_PER_COLUMN / distinct)


        for hist in histograms_list:

            percentage_sum_1 = 0
            percentage_sum_10 = 0
            percentage_sum_100 = 0


            for (i, entry) in enumerate(hist):
                cnt = entry['cnt']
                percent = cnt
                if i < 1:
                    percentage_sum_1 += percent
                if i < 10:
                    percentage_sum_10 += percent
                if i < 100:
                    percentage_sum_100 += percent

            n_values = len(hist)
            top_1_percentage.append(percentage_sum_1 )
            top_10_percentage.append(percentage_sum_10 / min(10, n_values))
            top_100_percentage.append(percentage_sum_100 / min(100, n_values))


        # make 3 boxplots in one figure
        data = [top_1_percentage, top_10_percentage, top_100_percentage, all]

        plt.boxplot(data, tick_labels=['Top 1', 'Top 10', 'Top 100', 'All'])
        plt.ylabel('%')
        plt.yscale('log')
        # nice y ticks
        plt.yticks([1, 10, 100, 1000, 10000, 100000], ['1', '10', '100', '1k', '10k', '100k'])

        plt.ylim(0.9, MAX_VALUES_TO_ANALYZE_PER_COLUMN * 1.1)
        plt.tight_layout()
        # save to latex assets dir
        path = os.path.join(LATEX_ASSETS_DIR, 'string_stats', 'duplicates', repo_group)
        os.makedirs(path, exist_ok=True)
        plt.savefig(os.path.join(path, f'{semantic_type_llm}_duplicates.pdf'), format='pdf', bbox_inches='tight', dpi=300)



def generate_section(con):
    
    generate_dups_plot(con)
    
    
    path = os.path.join(LATEX_ASSETS_DIR, 'string_stats')
    os.makedirs(path, exist_ok=True)
    get_string_prop_plots(con, path=path)

    local_assets_dir = 'assets/string_stats'

    multi_figure = get_multi_figure(
        label="img:string_properties",
        paths=[
            os.path.join(local_assets_dir, 'avg_length.pdf'),
            os.path.join(local_assets_dir, 'stddev_length.pdf'),
            os.path.join(local_assets_dir, 'count_filtered.pdf'),
            os.path.join(local_assets_dir, 'count_unfiltered.pdf'),
            # os.path.join(local_assets_dir, 'repeat_rate.pdf'),
        ],
        caption="String properties per semantic type. The filtered distinct chars only counts the number of chars that make up 95\\% of all chars.",
        captions=[
            "Avg. length",
            "Std. dev. length",
            "Distinct chars (filtered*)",
            "Distinct chars",
            # "Duplicates",
        ]
    )

    section = SECTION_TEMPLATE.format(
        STRING_PROPERTY_FIGURE=multi_figure,
    )

    with open(os.path.join(LATEX_GEN_DIR, SECTION_NAME), "w") as f:
        f.write(section)


if __name__ == "__main__":
    con = get_con()
    generate_section(con)
