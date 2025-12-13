import os

import numpy as np

from docs.gen.utils import get_figure, format_latex_string, get_multi_figure
from external.CompressionBenchmark.tools.benchmark import run_compression_benchmark
from src.config import get_con, LATEX_ASSETS_DIR, LATEX_GEN_DIR
from src.data_analysis.storage.storage_analysis import get_storage_percentage_table
import pandas as pd
import matplotlib.pyplot as plt
import duckdb

SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

section = """
In this section, we analyze the compressibility of the different groups in our string taxonomy to understand which 
types of strings compress well and which may benefit from further research. Our goal is to determine how much 
of the total storage footprint — both compressed and uncompressed — is occupied by each taxonomy category (RQ3).

{figure_compression_rate}

For this, we benchmarked four widely used compression algorithms that are known to perform well on string data. First, we consider 
standard dictionary compression, which builds a dictionary of frequently occurring strings and replaces each string 
with a shorter code. Next, we evaluate FSST, a specialized string compression algorithm that identifies repeated 
substrings, stores them in a dictionary, and replaces them with one-byte codes. We also include FSST12, a 
variant of FSST that uses 12-bit codes to support a larger symbol table. Additionally, we evaluate OnPair, 
which — similar to FSST — uses a dictionary and code assignments to replace substrings. 
In contrast, OnPair16 employs 16-bit codes and a novel algorithm for identifying repeated substrings.
Finally, we benchmark LZ4, a general-purpose compression algorithm.

{figure_value_sizes}

The effectiveness of a compression algorithm strongly depends on how it is integrated into the target system. 
Our benchmarking setup closely follows the approach used in a columnar \\ac{{OLAP}} \\ac{{DBMS}} like DuckDB: 
each column is compressed independently on a per–row-group basis, with each row group containing 128,000 values. 
Thus a single FSST symbol table is built and applied to one such 128k-value row group.
In contrast to Dictionary and FSST, LZ4 compresses an entire block of data at 
once and therefore does not support random access to individual values. As the block granularity, we 
follow DuckDB’s default vector size of 2048 values, which is also the block size used in their LZ4 
implementation.


{figure_storage_column_base_type}

By analyzing the compressed and uncompressed sizes of each column and grouping the columns by semantic type, 
we can quantify how much data of each type is stored in compressed and uncompressed form as depicted in \\cref{{fig:storage-column-base-type}}. 
This figure shows the percentage of columns, values, and storage size (uncompressed and compressed) per logical column type and 
can be read as follows: Each dataset corresponds to one row in each subfigure, with TPC being the first row in each section.
From this we can see that while `Text` columns make up 41% of all columns in TPC-[H, DS], 
they only account for 17% of all values. This is because dimension tables, which contain 
most of the `Text` columns (names, descriptions, categories), have far fewer rows than 
fact tables, which are dominated by `Integer` foreign keys and numeric measures. This 
reflects how normalization reduces data redundancy: rather than repeating text values 
across millions of fact table rows, the text is stored once in dimension tables and 
referenced via integer keys. For example, while the `store\_sales` has 23 rows of type `Int` and `DECIMAL`, 
it has a reference to the 160x smaller `item` table, where `Text` columns like `i\_item\_desc` and `i\_brand` make 
up 55% of all columns. Similar patterns can be observed in the IMDB schema, where 45% of `Text` columns contain 
only 30% of all values. 

However, this pattern does not hold for the Stack Overflow, Kaggle, and Hugging Face datasets. 
In Kaggle and Hugging Face, many datasets consist of a denormalized single wide table, leading to a higher share 
of text columns and values. In Stack Overflow, the data is dominated by user-generated content such as 
questions and answers, leading to a column and value percentage of 30%.
\insight{{In normalized schemas text columns are frequent but account for a small share of values, as textual attributes are concentrated in small dimension tables. 
Having many text columns does not necessarily mean having many text values.}}
In the section of \Cref{{fig:storage-column-base-type}}, we can now see the storage size occupied by each logical column type in
uncompressed form. The figure shows that in TPC-[H, DS] and IMDB, 
the compressed size of the `Text` columns make up twice as much as their share of values (37% vs. 17% in TPC, 65% vs. 33% in IMDB).
For SO, the 30% values that where strings are responsible for 95% of all uncompressed bytes stored. 
If we then look at the compressed size, we can see that for TPC-[H, DS], the share of `Text` columns on the stored bytes 
 decreases from uncompressed 37% to compressed 27%. For the other dataset, this decrease is not that drastic, only dropping 
 by around three percentage points. 
 
 {figure_storage_semantic_type}
 
 Why this is the case can be explained with \Cref{{fig:storage-semantic-type-llm}}, which follows the same structure as
 \Cref{{fig:storage-column-base-type}}, but instead of grouping by logical column type, it only considers the `text` columns and 
groups these by our taxonomy. We can see that for TPC-[H, DS], the `Categorical` strings (e.g., product categories, customer segments) make up the largest share of
with only 24% of all columns but 58% of all text values. As seen in \Cref{{fig:compression_per_semantic_type}}, `Categorical` compress very well. This is why they only make 
up 9% of the compressed storage size.

\insight{{While text columns might make up a small share of values, they can dominate storage size due 
to their larger average size per value. How much larger strongly varies by dataset. After compression, FullText
and Identifier columns make up ~ 80% of all Text bytes stored}}

Todo: Write something about kaggle

Todo: Maybe write about which percentage of the total storage is still variable size after compression. The idea
is that if you do DictCompression, not only is your string smaller but your dict codes are fixed size integers, much 
better for GPU processing.




"""


def main():
    get_storage_percentage_table('column_base_type', output_dir=LATEX_ASSETS_DIR)
    get_storage_percentage_table('semantic_type_llm', output_dir=LATEX_ASSETS_DIR)

    figure_storage_column_base_type = get_figure(
        path='assets/column_base_type.pdf',
        caption="""
        Storage analysis for the TPC-[H, DS], IMDB, Stackoverflow (SO), Kaggle, HuggingFace (HF) and DBPile. 
        We show the percentage of columns, values, and storage size (uncompressed and compressed) per logical column type. 
        TPC-[H, DS] is in the first row of each section: 41% of its columns are of type `Text`, but these columns only account for 17% of values. 
        The `Text` values then represent 37% of the uncompressed storage, but only 26% of the compressed storage. 
        \\Cref{fig:storage-column-base-type} shows why.
        """,
        label="fig:storage-semantic-type-llm",
    )
    figure_storage_semantic_type = get_figure(
        path='assets/semantic_type_llm.pdf',
        caption="Ratios for storage for all strings based on semantic type",
        label="fig:storage-column-base-type",
    )

    con = get_con(read_only=True)

    figure_value_sizes, figure_compression_rate = compression_per_semantic_type(con)
    description = section.format(
        figure_storage_semantic_type=figure_storage_semantic_type,
        figure_storage_column_base_type=figure_storage_column_base_type,
        figure_compression_rate=figure_compression_rate,
        figure_value_sizes=figure_value_sizes,
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
          SELECT 
            column_id, first(algorithm ORDER BY compressed_size) as algorithm, 
            MIN(uncompressed_size) /  MIN(compressed_size) as compression_rate,
            MIN(uncompressed_size / n_rows_not_empty) as uncompressed_size,
            MIN(compressed_size / n_rows_not_empty) as compressed_size
          FROM "columns_compression_results"
          GROUP BY column_id
        )
        SELECT semantic_type_llm, algorithm, COUNT(*) as cnt, 
            list(compression_rate) as "compression_rates",
            list(uncompressed_size) as "uncompressed_sizes",
            list(compressed_size) as "compressed_sizes"
        FROM best_algo_per_column as algo_data
        JOIN columns ON columns.id = algo_data.column_id
        GROUP BY ALL
        HAVING semantic_type_llm IS NOT NULL AND semantic_type_llm NOT IN ('Other', 'Test')
        ORDER BY ALL
        """).fetchdf()

    n_algos = df['algorithm'].nunique()
    all_algorithms = df['algorithm'].unique()  # Get all algorithms across entire dataset

    uncompressed_sizes = {}
    compressed_size = {}

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

                algo_subset = subset[subset['algorithm'] == algo]
                rates = algo_subset['compression_rates'].iloc[0]

                if algo in list(best_algorithms_in_subset):
                    row = best_subset[best_subset['algorithm'] == algo]

                    if semantic_type not in uncompressed_sizes:
                        uncompressed_sizes[semantic_type] = list(row['uncompressed_sizes'].iloc[0])
                        compressed_size[semantic_type] = list(row['compressed_sizes'].iloc[0])
                    else:
                        uncompressed_sizes[semantic_type] += list(row['uncompressed_sizes'].iloc[0])
                        compressed_size[semantic_type] += list(row['compressed_sizes'].iloc[0])

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
            plt.plot(x_pos, median_val, 'D', color='red', markersize=3 + 7 * (percentage / 100), markeredgewidth=1,
                     markeredgecolor='black')

        plt.yscale('log')
        plt.ylim(1, 100)
        plt.yticks(
            [0.2, 0.5, 1, 2, 5, 10, 20, 50, 100],
            ['0.2x', '0.5x', '1x', '2x', '5x', '10x', '20x', '50x', '100x'],
            fontsize=12,
        )
        plt.xticks(rotation=90, fontsize=12)
        plt.tight_layout()

        path = os.path.join(LATEX_ASSETS_DIR, 'compression', f'compression_{semantic_type}.pdf')
        os.makedirs(os.path.dirname(path), exist_ok=True)
        plt.savefig(path)
        plt.close()

        paths.append(path)
        captions.append(semantic_type)

    def get_boxplot_range(data_dict, whisker: bool):
        """Calculate min/max based on boxplot whiskers (excluding outliers)"""
        all_whisker_values = []
        all_complete_ranges = []

        for sizes in data_dict.values():
            if len(sizes) == 0:
                continue
            q1 = np.percentile(sizes, 25)
            q3 = np.percentile(sizes, 75)
            iqr = q3 - q1
            lower_whisker = max(min(sizes), q1 - 1.5 * iqr)
            upper_whisker = min(max(sizes), q3 + 1.5 * iqr)
            mean = np.mean(sizes)

            local_min = min(sizes)
            local_max = max(sizes)
            all_complete_ranges.extend([local_min, local_max, mean])
            all_whisker_values.extend([lower_whisker, upper_whisker, mean])

        if whisker:
            return min(all_whisker_values), max(all_whisker_values)
        else:
            return min(all_complete_ranges), max(all_complete_ranges)

    # Calculate global min and max based on boxplot whiskers (no outliers)
    whisker = True
    uncompressed_min, uncompressed_max = get_boxplot_range(uncompressed_sizes, whisker=whisker)
    compressed_min, compressed_max = get_boxplot_range(compressed_size, whisker=whisker)

    global_min = min(uncompressed_min, compressed_min) * 0.9
    global_max = max(uncompressed_max, compressed_max) * 2.0

    plt.figure(figsize=(len(uncompressed_sizes) * 0.37 + 0.7, 3.5))

    # Get the keys and sort them
    keys = list(uncompressed_sizes.keys())
    keys.sort()

    sizes_paths = []
    sizes_captions = []

    # Create both plots
    for filename, data_dict, label in [
        ('uncompressed_sizes', uncompressed_sizes, 'Uncompressed'),
        ('compressed_sizes', compressed_size, 'Compressed')
    ]:
        plt.grid(which='major', axis='y', linestyle='--', linewidth=0.5)

        for i, stype in enumerate(keys):
            data = data_dict[stype]
            position = i + 1
            plt.boxplot(data, positions=[position],
                        widths=0.4, showfliers=not whisker, showmeans=True)

            # Add mean value as text
            mean_val = sum(data) / len(data)
            plt.text(position + 0.27, mean_val, f'{mean_val:.0f}',
                     va='center', fontsize=10, color='black', rotation=90)

        plt.yscale('log')

        # make the y ticks powers of 2 from global_min to global_max
        y_ticks = []
        y_labels = []
        val = 0.0625 / 2
        while val < global_max:
            if val >= global_min:
                y_ticks.append(val)
                if val >= 1:
                    y_labels.append(f'{int(val)}')
                else:
                    # add as fraction
                    divident = 1 / val
                    y_labels.append(f'1/{int(divident)}')
            val *= 4
        plt.yticks(y_ticks, y_labels)
        plt.ylim(global_min, global_max)
        plt.ylabel('Size in Bytes')

        plt.xlim(0.5, len(data_dict) + 0.7)
        plt.xticks(
            range(1, len(data_dict) + 1),
            list(data_dict.keys()),
            rotation=90,
            fontsize=12
        )
        plt.tight_layout()

        path = os.path.join(LATEX_ASSETS_DIR, 'compression', f'{filename}.pdf')
        os.makedirs(os.path.dirname(path), exist_ok=True)
        plt.savefig(path)
        plt.clf()

        sizes_paths.append(path)
        sizes_captions.append(label)

    size_per_semantic_type_figure = get_multi_figure(
        two_column=False,
        label="fig:size_per_semantic_type",
        caption="Sizes per semantic type for uncompressed and compressed data. Mean sizes are indicated as text next to each boxplot.",
        paths=sizes_paths,
        captions=sizes_captions
    )

    compression_per_semantic_type_figure = get_multi_figure(
        two_column=True,
        label="fig:compression_per_semantic_type",
        caption="Compression ratios by semantic type and algorithm. Percentages indicate how often each algorithm achieved the best compression. Red diamonds show the median compression ratio in the columns where the algorithm performed best, with symbol size proportional to its share of best-compressed columns.",
        paths=paths,
        captions=captions
    )

    return size_per_semantic_type_figure, compression_per_semantic_type_figure


if __name__ == "__main__":
    main()
