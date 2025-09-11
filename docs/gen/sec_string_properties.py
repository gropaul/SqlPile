import os

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from docs.gen.utils import get_multi_figure
from src.config import get_con, LATEX_ASSETS_DIR, LATEX_GEN_DIR

SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

plt.rcParams.update({
    "text.usetex": True,  # use LaTeX for all text
    "font.family": "serif",  # set serif font
    "font.serif": ["Computer Modern Roman"],  # or another LaTeX font
})

# Configure once
SEMANTIC_TYPES = [
    "Name", "Location", "DateTime",
    "Identifier", "FullText", "Category", "Contact", "Semistructured",
]
SEMANTIC_TYPES_SQL = ", ".join(f"'{t}'" for t in SEMANTIC_TYPES)


def _build_query(metric_col: str, p_low: float = 0.05, p_high: float = 0.95) -> str:
    """
    Build a query that filters rows per semantic_type_llm to the [p_low, p_high] percentiles
    of the chosen metric column.
    """
    return f"""
        WITH data AS (
            SELECT 
                unify_llm_type(semantic_type_llm) as semantic_type_llm, 
                counts:  list_transform(char_histogram, (x -> x.cnt)), 
                total: list_sum(counts),
                percentages:  list_filter(counts, (x -> x / total > 0.01)),
                count_filtered: len(percentages),
                count_unfiltered: len(char_histogram),
                empty_or_null: count_null + count_empty,
                {metric_col} AS metric_value
            FROM column_stats_text AS stats
            JOIN "columns" ON columns.id = stats.column_id
            JOIN tables ON columns.table_id = tables.id
            JOIN (
                SELECT column_id AS column_id_llm, semantic_type AS semantic_type_llm
                FROM '/Users/paul/workspace/SqlPile/src/data_analysis/*.csv'
            ) AS st ON st.column_id_llm = columns.id
            WHERE semantic_type_llm IN ({SEMANTIC_TYPES_SQL})
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
        order=SEMANTIC_TYPES,
        palette="Set3",
        showfliers=True,
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
    boxplot_by_type(con, "repeat_rate", path=path, ylabel="Duplicates per Value")
    boxplot_by_type(con, "empty_or_null", path=path, ylabel="Null Values", log_scale=False)


SECTION_TEMPLATE = """
{STRING_PROPERTY_FIGURE}
"""


def generate_section(con):
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
            os.path.join(local_assets_dir, 'repeat_rate.pdf'),
        ],
        caption="String properties per semantic type.",
        captions=[
            "Avg. length",
            "Std. dev. length",
            "Distinct chars (filtered)",
            "Distinct chars",
            "Duplicates",
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
