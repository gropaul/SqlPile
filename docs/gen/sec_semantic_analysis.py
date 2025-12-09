import os
import re
from docs.gen.utils import get_figure, format_latex_string, get_multi_figure
from src.config import get_con, LATEX_ASSETS_DIR, LATEX_GEN_DIR
from src.data_analysis.usage_plots import column_physical_type_usage_plot, column_semantic_type_llm_usage_plot

SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

section = """
In the following, we will discuss how which semantic type is used in which operator for different benchmarks (TPC-H, TPC-DS, SQLStorm).

{figure_join}

{figure_aggregate}

{figure_group_by}

{figure_order_by}

{figure_where}

{figure_payload}

- Describe which semantic type is used in which operator. Go through operator by operator.
- Highlight the differences between the different benchmarks (TPC-H, TPC-DS, SQLStorm)
- Mention that boolean and numeric are merged to other types. 
- Create a table with examples of the semantic type and how often it occured. Maybe add like a 2nd level of detail, e.g., for location: city, country, address, ...
- What type of expressions are used for which operator and semantic type? 
- What is the difference between the expressions in the different benchmarks?
"""

physical_path = os.path.join(LATEX_ASSETS_DIR, 'column_usage', 'physical_type')
logical_path = os.path.join(LATEX_ASSETS_DIR, 'column_usage', 'semantic_type')

def get_semantic_figure(type: str, caption: str):
    return get_multi_figure(
        percentages=[0.238 * 0.98, 0.761 * 0.98], two_column=False,
        caption=caption,
        label="fig:semantic_type_" + type,
        paths=[
            os.path.join(physical_path, f'column_cnt_{type}.pdf'),
            os.path.join(logical_path, f'column_cnt_{type}.pdf'),
        ],
        captions=["Logical", "Taxonomy of Text Columns"]
    )


def main():
    con = get_con(read_only=True)


    os.makedirs(physical_path, exist_ok=True)
    os.makedirs(logical_path, exist_ok=True)

    column_physical_type_usage_plot(con, output_dir=physical_path)
    column_semantic_type_llm_usage_plot(con, output_dir=logical_path)

    figure_join = get_semantic_figure( type='join_key',
        caption='Distribution of semantic types used in columns involved in JOIN operations across different benchmarks.'
    )

    figure_aggregate = get_semantic_figure( type='aggregate',
        caption='Distribution of semantic types used in columns involved in AGGREGATE operations across different benchmarks.'
    )

    figure_group_by = get_semantic_figure( type='group_key',
        caption='Distribution of semantic types used in columns involved in GROUP BY operations across different benchmarks.'
    )

    figure_order_by = get_semantic_figure( type='order_key',
        caption='Distribution of semantic types used in columns involved in ORDER BY operations across different benchmarks.'
    )

    figure_where = get_semantic_figure( type='filter',
        caption='Distribution of semantic types used in columns involved in WHERE operations across different benchmarks.'
    )

    figure_payload = get_semantic_figure( type='payload_column',
        caption='Distribution of semantic types used in columns in materializing operators'
    )

    description = section.format(
        figure_join=figure_join,
        figure_aggregate=figure_aggregate,
        figure_group_by=figure_group_by,
        figure_order_by=figure_order_by,
        figure_where=figure_where,
        figure_payload=figure_payload,

    )

    description = format_latex_string(description)

    path = os.path.join(LATEX_GEN_DIR, SECTION_NAME)
    with open(path, "w") as f:
        f.write(description)


if __name__ == "__main__":
    main()
