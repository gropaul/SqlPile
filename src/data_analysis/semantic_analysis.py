from src.config import DATABASE_PATH
import duckdb

def main():
    con = duckdb.connect(DATABASE_PATH)

    data = con.execute("""
                SELECT semantic_type, COUNT(*) as cnt
                FROM value_stats
                GROUP BY semantic_type
                HAVING cnt > 200
                ORDER BY cnt
                """).fetchall()

    # create a pie chart of the data
    import matplotlib.pyplot as plt
    import pandas as pd

    # Enable LaTeX and increase font sizes
    plt.rcParams.update({
        "text.usetex": True,
        "font.size": 18,
        "axes.titlesize": 24,
        "legend.fontsize": 16,
        "figure.titlesize": 24
    })

    df = pd.DataFrame(data, columns=["semantic_type", "count"])
    df.set_index("semantic_type", inplace=True)
    df.plot.pie(
        y="count",
        autopct='%1.1f\\%%',
        figsize=(10, 10),
        legend=False,
        textprops={'fontsize': 18}
    )
    plt.title(r"\textbf{Distribution of Semantic Types}")
    plt.ylabel("")  # Remove the y-label for clarity
    plt.tight_layout()
    plt.savefig("/Users/paul/workspace/SqlPile/src/data_analysis/semantic_type_distribution.png", dpi=300)
    plt.show()
    print("Pie chart saved as semantic_type_distribution.png")


if __name__ == "__main__":
    main()
