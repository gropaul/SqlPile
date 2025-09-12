


if __name__ == "__main__":

    # load /Users/paul/workspace/SqlPile/src/sql_analysis/execution/compression_benchmark_output.csv
    import pandas as pd
    df = pd.read_csv('/Users/paul/workspace/SqlPile/src/sql_analysis/execution/compression_benchmark_output.csv')
    # columns: table,column,uncompressed_size,n_rows,n_rows_not_empty,algorithm,compressed_size,compression_time_ms,decompression_time_ms

    # calculate compression ratio
    df['compression_ratio'] = df['uncompressed_size'] / df['compressed_size']

    # create a boxplot of compression ratio by algorithm
    import matplotlib.pyplot as plt
    import seaborn as sns
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df, x='algorithm', y='compression_ratio')
    plt.title('Compression Ratio by Algorithm')
    plt.xlabel('Algorithm')
    plt.ylabel('Compression Ratio (Uncompressed Size / Compressed Size)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # print average compression ratio by algorithm, median compression ratio by algorithm, min and max compression ratio by algorithm
    summary = df.groupby('algorithm')['compression_ratio'].agg(['mean', 'median', 'min', 'max']).reset_index()
    print(summary)
