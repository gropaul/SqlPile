import os

from docs.gen.utils import format_latex_string
from src.config import get_con, LATEX_GEN_DIR
from src.data_analysis.dataset_stats import get_column_type_table

SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

section = """
{column_type_tbl}

\Cref{{tab:column-types}} shows the most common logical column types in DBPile, IMDB, Stackoverflow, TPC-[H, DS], Kaggle and HuggingFace. 
For DBPile. the most common column type is `Text`. For Stackoverflow, IMDB and the TPC-[H, DS] benchmarks, the most common type is `Integer`,
while Kaggle and HuggingFace feature a lot of `Float` columns. This comes from many wide ML feature tables in these datasets.

Todo: Add Redset, Snowflake data here. 
"""


def main():
    con = get_con(read_only=True)
    column_type_tbl = get_column_type_table(
        con,
        output_format='latex',
        label='tab:column-types',
        caption="""
        Most common logical column types across DBPile, IMDB, Stack Overflow (SO), TPC-H/DS, HuggingFace (HF), and 
        Redshift~\cite{van_renen_why_2024}. In DBPile and Redshift, the majority of columns are of type `Text`,
         whereas TPC-H and TPC-DS are dominated by `Integer` columns. HuggingFace contains a comparatively 
         large share of `Float` columns, reflecting the prevalence of wide machine-learning feature tables.
        """
    )

    filled_text = section.format(
        column_type_tbl=column_type_tbl
    )

    output_path = os.path.join(LATEX_GEN_DIR, SECTION_NAME)

    with open(output_path, "w") as f:
        f.write(format_latex_string(filled_text))
    return filled_text


if __name__ == "__main__":
    print(main())
