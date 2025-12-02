import os
from docs.gen.utils import get_figure, format_latex_string, get_multi_figure
from src.config import get_con, LATEX_ASSETS_DIR, LATEX_GEN_DIR
from src.data_analysis.storage.storage_analysis import get_storage_percentage_table
import pandas as pd
import matplotlib.pyplot as plt
import duckdb

SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

section = """
In this section, we analyze the compressibility of the different groups in our string taxonomy to investigate
which types of strings compress well and which could benefit from further research. \\cref{{img:compression_per_semantic_type}}
shows the compression ratios achieved by different algorithms for each group in our taxonomy.

The performance of a compression algorithm heavily depends on how it is used. As we want to foucs on

{figure_compression_rate}

By analyzing the compressed and uncompressed sizes of each column and grouping the columns by semantic type, 
we can quantify how much data of each type is stored in compressed and uncompressed form. As a first step, we 
show how many columns of each semantic type occur across all datasets and how many values they contain. We then 
examine how much storage space the values of each semantic type occupy both in their uncompressed form and when 
compressed using the best available algorithm.
The results of these analyses are shown in \\cref{{fig:storage-semantic-type-llm}} and \\cref{{fig:storage-column-base-type}}.

\\cref{{fig:storage-semantic-type-llm}} shows ... 

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
        figure_storage_column_base_type=figure_storage_column_base_type,
        figure_compression_rate=compression_per_semantic_type(con)
    )

    description = format_latex_string(description)

    path = os.path.join(LATEX_GEN_DIR, SECTION_NAME)
    with open(path, "w") as f:
        f.write(description)


def compression_per_semantic_type(con: duckdb.DuckDBPyConnection) -> str:

    df = con.sql("""
        WITH all_algo_per_column AS (
          SELECT column_id, algorithm, uncompressed_size / compressed_size as compression_rate
          FROM "columns_compression_results"
        ) 
        SELECT semantic_type_llm, algorithm, COUNT(*) as cnt, list(compression_rate) as "compression_rates"
        FROM all_algo_per_column as algo_data
        JOIN columns ON columns.id = algo_data.column_id
        GROUP BY ALL
        HAVING semantic_type_llm IS NOT NULL AND semantic_type_llm NOT IN ('Other', 'Test')
        ORDER BY ALL
    """).fetchdf()

    best_df = con.sql("""
        WITH best_algo_per_column AS (
          SELECT column_id, first(algorithm ORDER BY compressed_size) as algorithm, MIN(uncompressed_size) /  MIN(compressed_size) as compression_rate
          FROM "columns_compression_results"
          GROUP BY column_id
        )
        SELECT semantic_type_llm, algorithm, COUNT(*) as cnt, list(compression_rate) as "compression_rates"
        FROM best_algo_per_column as algo_data
        JOIN columns ON columns.id = algo_data.column_id
        GROUP BY ALL
        HAVING semantic_type_llm IS NOT NULL AND semantic_type_llm NOT IN ('Other', 'Test')
        ORDER BY ALL
        """).fetchdf()

    n_algos = df['algorithm'].nunique()
    all_algorithms = df['algorithm'].unique()  # Get all algorithms across entire dataset

    paths = []
    captions = []
    # one boxplot per semantic type
    for semantic_type in df['semantic_type_llm'].unique():
        subset = df[df['semantic_type_llm'] == semantic_type]
        algorithms_in_subset = subset['algorithm'].unique()

        best_subset = best_df[best_df['semantic_type_llm'] == semantic_type]
        best_algorithms_in_subset = best_subset['algorithm'].unique()

        data = []
        labels = []
        positions_with_data_median = []
        positions_without_data = []

        best_subset_sum = best_subset['cnt'].sum()

        for i, algo in enumerate(all_algorithms):
            if algo in algorithms_in_subset:
                rates = subset[subset['algorithm'] == algo]['compression_rates'].iloc[0]

                if algo in list(best_algorithms_in_subset):
                    row = best_subset[best_subset['algorithm'] == algo]
                    best_cnt = row['cnt'].iloc[0]
                    ratios = row['compression_rates'].iloc[0]
                    percentage = (best_cnt / best_subset_sum) * 100 if best_subset_sum > 0 else 0

                    median_ratio = pd.Series(ratios).median()
                    positions_with_data_median.append((i + 1, median_ratio, percentage))

                else:
                    percentage = 0
                    positions_without_data.append(i + 1)

                labels.append(f"{algo} ({round(percentage)}\%)")
                data.append(rates)

        plt.figure(figsize=(n_algos * 0.30 + 0.7, 3.5))

        # make all text bigger

        # Plot boxplots for algorithms with data
        bp = plt.boxplot(data, tick_labels=labels, showfliers=False, widths=0.5)

        # Add crosses for algorithms without data
        for pos in positions_without_data:
            plt.plot(pos, 1, 'x', color='black', markersize=4, markeredgewidth=1)  # Cross at middle of log scale

        for (x_pos, median_val, percentage) in positions_with_data_median:
            # also add a red line for the median
            plt.plot([x_pos - 0.25, x_pos + 0.25], [median_val, median_val], color='red', linewidth=1)
            plt.plot(x_pos, median_val, 'D', color='red', markersize= 3 + 7 * (percentage / 100), markeredgewidth=1, markeredgecolor='black')


        plt.yscale('log')
        plt.ylim(1, 100)
        plt.yticks(
            [0.2, 0.5, 1, 2, 5, 10, 20, 50, 100],
            ['0.2x', '0.5x', '1x', '2x', '5x', '10x', '20x', '50x', '100x'],
            fontsize=12,
        )
        plt.xticks(rotation=90, fontsize=12)
        plt.tight_layout()

        path = os.path.join(LATEX_ASSETS_DIR, 'compression',f'compression_{semantic_type}.pdf')
        os.makedirs(os.path.dirname(path), exist_ok=True)
        plt.savefig(path)
        plt.close()

        paths.append(path)
        captions.append(semantic_type)


    return get_multi_figure(
        label="img:compression_per_semantic_type",
        caption="Compression ratios by semantic type and algorithm. Percentages indicate how often each algorithm achieved the best compression. Red diamonds show the median compression ratio in the columns where the algorithm performed best, with symbol size proportional to its share of best-compressed columns.",
        paths=paths,
        captions=captions
    )





if __name__ == "__main__":
    generate_storage_analysis()




