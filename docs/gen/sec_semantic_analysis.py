import os

from docs.gen.utils import get_figure, format_latex_string
from src.config import get_con, LATEX_ASSETS_DIR, GENERATED_SECTIONS_DIR
from src.data_analysis.usage_plots import column_physical_type_usage_plot, column_semantic_type_llm_usage_plot


SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

section = """
As strings can be used as a catch it all type, depending on what the user is storing, the properties of the string can 
be very different. To analyze strings depending on their what they are used for, we used the `{MODEL_NAME}` model to 
determine the semantic type of each column based on the table and column name and a sample of the values in the column.

{figure_column_semantic_type_usage}
"""


def gen_semantic_analysis():
    con = get_con(read_only=True)

    column_semantic_type_llm_usage_plot(con, output_dir=LATEX_ASSETS_DIR)
    figure_column_physical_type_usage = get_figure(
        path='assets/column_usage/semantic_type_llm/column_cnt.pdf',
        description="Distribution of the semantic types of different benchmarks.",
        caption="Distribution of the semantic types of different benchmarks.",
        label="fig:column_semantic_type_usage",
    )

    description = section.format(
        figure_column_semantic_type_usage=figure_column_physical_type_usage,
    )

    description = format_latex_string(description)

    path = os.path.join(GENERATED_SECTIONS_DIR, SECTION_NAME)
    with open(path, "w") as f:
        f.write(description)


if __name__ == "__main__":
    gen_semantic_analysis()


