import os

from docs.gen.utils import format_latex_string
from src.config import get_con, LATEX_GEN_DIR
from src.data_analysis.dataset_stats import get_column_type_table

SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

section = """
{column_type_tbl}
"""

def main():
    con = get_con(read_only=True)
    column_type_tbl = get_column_type_table(
        con,
        output_format='latex',
        label='tab:column-types',
        caption='Most common logical column types in DBPile, IMDB, Stackoverflow (SO), TPC-[H, DS], '
                'Kaggle and HuggingFace (HF). For DBPile, the most columns are of type `Text` while '
                'for TPC-[H, DS] benchmarks, the most common type is `Integer`. Kaggle and HuggingFace '
                'feature a lot of float columns, coming from wide ML feature tables.'
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