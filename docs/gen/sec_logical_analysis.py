import os

from docs.gen.utils import format_latex_string, get_figure
from src.config import LATEX_GEN_DIR, get_con, LATEX_ASSETS_DIR
from src.data_analysis.dataset_stats import get_operator_table
from src.data_analysis.usage_plots import column_physical_type_usage_plot

SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")
LABEL_TABLE_OPERATOR_COUNTS = "tab:op-usage"

text = """
{op_usage_table}

We analyzed the operator usage of SqlPile queries and compared them to the TPC-H and TPC-DS benchmarks. As depicted in 
\\cref{{tab:op-usage}}, SqlPile queries use far fewer logical operators than the TPC benchmarks.
This comes from the fact that a lot of the queries in SqlPile are simple `SELECT` statements from a transactional workload,
while the TPC benchmarks resemble an analytical workload with complex joins and aggregations.

We analyzed the distribution of logical column types used in SqlPile, which are shown in \\cref{{tab:column-types}}, which is 
similar to findings in Redset~\\cite{{van_renen_why_2024}} and the Public BI~\\cite{{vogelsgesang_get_2018}} benchmark.

{figure_column_physical_type_usage}

"""


def main():
    con = get_con(read_only=True)

    operator_tbl = get_operator_table(con, output_format='latex', label=LABEL_TABLE_OPERATOR_COUNTS,
                                      caption='Logical Operators per query for SqlPile and TPC-[H, DS] benchmarks. The SQLPile queries are far less complex.')\

    column_physical_type_usage_plot(con, output_dir=LATEX_ASSETS_DIR)
    figure_column_physical_type_usage = get_figure(
        path='assets/column_usage/physical_type/column_cnt.pdf',
        caption="Distribution of physical column types in SqlPile.",
        label="fig:column-physical-type-usage"
    )

    description = text.format(
        op_usage_table=operator_tbl,
        figure_column_physical_type_usage=figure_column_physical_type_usage
    )
    description = format_latex_string(description)

    path = os.path.join(LATEX_GEN_DIR, SECTION_NAME)
    with open(path, "w") as f:
        f.write(description)

    print(f"Dataset description written to {path}")
    return description


if __name__ == "__main__":
    print(main())
