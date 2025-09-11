import os
import re
from docs.gen.utils import get_figure, format_latex_string
from src.config import get_con, LATEX_ASSETS_DIR, LATEX_GEN_DIR
from src.data_analysis.usage_plots import column_physical_type_usage_plot, column_semantic_type_llm_usage_plot


SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

section = """
In the following, we will discuss how which semantic type is used in which operator for different benchmarks (TPC-H, TPC-DS, SQLStorm).

{figure_column_semantic_type_usage}

- Describe which semantic type is used in which operator. Go through operator by operator.
- Highlight the differences between the different benchmarks (TPC-H, TPC-DS, SQLStorm)
- Mention that boolean and numeric are merged to other types. 
- Create a table with examples of the semantic type and how often it occured. Maybe add like a 2nd level of detail, e.g., for location: city, country, address, ...
- What type of expressions are used for which operator and semantic type? 
- What is the difference between the expressions in the different benchmarks?
"""


def generate_semantic_analysis():
    con = get_con(read_only=True)

    column_semantic_type_llm_usage_plot(con, output_dir=LATEX_ASSETS_DIR)
    figure_column_physical_type_usage = get_figure(
        path='assets/column_usage/semantic_type_llm/column_cnt.pdf',
        caption="Distribution of the semantic types of different benchmarks.",
        label="fig:column_semantic_type_usage",
    )

    description = section.format(
        figure_column_semantic_type_usage=figure_column_physical_type_usage,
    )

    description = format_latex_string(description)

    path = os.path.join(LATEX_GEN_DIR, SECTION_NAME)
    with open(path, "w") as f:
        f.write(description)


if __name__ == "__main__":
    generate_semantic_analysis()


