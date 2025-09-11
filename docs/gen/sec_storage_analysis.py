import os
from docs.gen.utils import get_figure, format_latex_string
from src.config import get_con, LATEX_ASSETS_DIR, LATEX_GEN_DIR
from src.data_analysis.storage.storage_analysis import get_storage_percentage_table


SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

section = """
In this section, we want to analyze how much storage is used for different logical types and semantic types.

{figure_storage_column_base_type}

{figure_storage_semantic_type}

"""


def generate_storage_analysis():

    get_storage_percentage_table('column_base_type', output_dir=LATEX_ASSETS_DIR)
    figure_storage_semantic_type = get_figure(
        path='assets/column_base_type.pdf',
        caption="Analysis on the storage per logical types",
        label="fig:storage-semantic-type-llm",
    )

    get_storage_percentage_table('semantic_type_llm', output_dir=LATEX_ASSETS_DIR)
    figure_storage_column_base_type = get_figure(
        path='assets/semantic_type_llm.pdf',
        caption="Ratios for storage for all strings based on semantic type",
        label="fig:storage-column-base-type",
    )

    con = get_con(read_only=True)

    description = section.format(
        figure_storage_semantic_type=figure_storage_semantic_type,
        figure_storage_column_base_type=figure_storage_column_base_type
    )

    description = format_latex_string(description)

    path = os.path.join(LATEX_GEN_DIR, SECTION_NAME)
    with open(path, "w") as f:
        f.write(description)


if __name__ == "__main__":
    generate_storage_analysis()



