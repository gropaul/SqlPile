import os

import pandas as pd

from src.config import get_con, LATEX_ASSETS_DIR

MAX_CHARS = 200
MAX_EXAMPLES_PER_TYPE = 5
MAX_VALUES_PER_EXAMPLE = 3


def semantic_type_percentages():

    con = get_con(read_only=True)

    df = con.sql(f"""
            WITH aggregates AS (
                SELECT
                    get_group(repos.repo_url) as repo_group,
                    semantic_type_llm,
                    semantic_type_llm_subtype,
                    COUNT(*) as type_count
                FROM columns
                JOIN tables ON tables.id = columns.table_id
                JOIN repos ON repos.id = tables.repo_id
                WHERE repo_group != 'Excluded'
                    AND semantic_type_llm IS NOT NULL
                    AND semantic_type_llm_subtype IS NOT NULL
                    AND semantic_type_llm != 'Other'
                GROUP BY ALL 
                UNION ALL
                SELECT
                    get_group(repos.repo_url) as repo_group,
                    semantic_type_llm,
                    'N/A'   as semantic_type_llm_subtype,
                    COUNT(*) as type_count
                FROM columns
                JOIN tables ON tables.id = columns.table_id
                JOIN repos ON repos.id = tables.repo_id
                WHERE repo_group != 'Excluded' AND  semantic_type_llm IN ('Other' , 'Test')
                GROUP BY repo_group, semantic_type_llm
            ), 
            totals AS (
                SELECT
                    repo_group,
                    SUM(type_count) as total_count
                FROM aggregates
                GROUP BY repo_group
            ),
            percentages AS (
                SELECT
                    a.repo_group,
                    a.semantic_type_llm,
                    a.semantic_type_llm_subtype,
                    ROUND(100.0 * a.type_count / t.total_count, 1) as type_percentage,
                FROM aggregates a
                JOIN totals t ON a.repo_group = t.repo_group
            ),
            pivoted AS (
                PIVOT percentages
                ON repo_group
                USING SUM(type_percentage)
            )
            SELECT
                *
            FROM pivoted   
        """).df()

    # rename semantic_type_llm to Semantic Type and semantic_type_llm_subtype to Subtype
    df = df.rename(columns={
        "semantic_type_llm": "Semantic Type",
        "semantic_type_llm_subtype": "Subtype"
    })

    # add one example column, that is empty for now
    # order the columns:

    datasets = ["TPC-H", "TPC-DS", "IMDB", "Stack Overflow", "Kaggle", "HuggingFace", "DBPile"]
    columns = ["Semantic Type", "Subtype"] + datasets
    # check if all columns exist
    for col in columns:
        if col not in df.columns:
            df[col] = None
            print(f"Warning: Column {col} not found in dataframe, adding it with None values.")
            print(f"Warning: Column {col} not found in dataframe, adding it with None values.")
            print(f"Warning: Column {col} not found in dataframe, adding it with None values.")
    df = df[columns]

    path = os.path.join(LATEX_ASSETS_DIR, "semantic_type_percentages.tex")

    # add one column that has the average percentage across all datasets
    # replace all NaN with 0 for the purpose of averaging
    df[datasets] = df[datasets].fillna(0)
    df["Average"] = df[datasets].mean(axis=1)

    datasets = datasets + ["Average"]

    unique_types = df["Semantic Type"].unique()
    original_df = df.copy()  # so we don't sum over rows we add later
    rows_to_add = []

    for semantic_type in unique_types:
        type_rows = original_df[original_df["Semantic Type"] == semantic_type]
        if len(type_rows) > 1:
            # start from the first row as a template
            sum_row = type_rows.iloc[0].copy()
            sum_row["Subtype"] = "All"
            for col in datasets:
                sum_row[col] = type_rows[col].sum()
            rows_to_add.append(sum_row)


    # add all "All" rows at once
    df = pd.concat([df, pd.DataFrame(rows_to_add)], ignore_index=True)

    df = df.sort_values(by=["Semantic Type", "DBPile"], ascending=[True, False])

    # format all the datasets columns as percentages with 1 decimal place
    for col in datasets:
        df[col] = df[col].apply(lambda x: str(round(x, 1)) + '\%' if( pd.notnull(x) and pd.notna(x)  and x is not None and x != 0) else '-')


    # for all columns where there is not an 'All' or 'N/A', make the type empty
    def format_subtype(row):
        if row["Subtype"] not in ["All", "N/A"]:
            return ""
        return row["Semantic Type"]
    df["Semantic Type"] = df.apply(format_subtype, axis=1)
    # save this as a latex table
    with open(path, "w") as f:
        f.write(df.to_latex(index=False, float_format="%.2f", na_rep="-"))




def semantic_type_example():
    con = get_con(read_only=True)



    # First, get all combinations with their examples and percentages
    df = con.sql(f"""
            WITH ranked_columns AS (
                SELECT
                    semantic_type_llm,
                    semantic_type_llm_subtype,
                    tables.table_name as table_name,
                    columns.column_name as column_name,
                    columns.id as column_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY semantic_type_llm, semantic_type_llm_subtype
                        ORDER BY columns.id
                    ) as rn
                FROM columns
                JOIN tables ON tables.id = columns.table_id
                JOIN table_values_count as tvc ON tvc.table_id = tables.id
                WHERE tvc.count > 4000
                ORDER BY hash(column_id)
            ),
            examples AS (
                SELECT
                    rc.semantic_type_llm,
                    rc.semantic_type_llm_subtype,
                    rc.table_name,
                    rc.column_name,
                    LIST_DISTINCT(LIST(('"' || cv.value || '"') ORDER BY cv.column_id))[:{MAX_VALUES_PER_EXAMPLE}] as sample_values
                FROM ranked_columns rc
                JOIN column_values cv ON rc.column_id = cv.column_id
                WHERE rc.rn <= {MAX_EXAMPLES_PER_TYPE}
                GROUP BY rc.semantic_type_llm, rc.semantic_type_llm_subtype, rc.table_name, rc.column_name, rc.rn
            ),
            totals AS (
                SELECT
                    COUNT(*) as total_columns
                FROM columns
                WHERE semantic_type_llm IS NOT NULL
            ),
            type_counts AS (
                SELECT
                    semantic_type_llm,
                    COUNT(*) as type_count
                FROM columns
                WHERE semantic_type_llm IS NOT NULL
                GROUP BY semantic_type_llm
            ),
            subtype_counts AS (
                SELECT
                    semantic_type_llm,
                    semantic_type_llm_subtype,
                    COUNT(*) as subtype_count
                FROM columns
                WHERE semantic_type_llm IS NOT NULL
                    AND semantic_type_llm_subtype IS NOT NULL
                GROUP BY semantic_type_llm, semantic_type_llm_subtype
            )
            SELECT
                e.semantic_type_llm,
                e.semantic_type_llm_subtype,
                ROUND(100.0 * tc.type_count / t.total_columns, 2) as type_percentage,
                ROUND(100.0 * sc.subtype_count / tc.type_count, 2) as subtype_percentage,
                LIST(
                    e.table_name || '.' || e.column_name || ': ' ||
                    array_to_string(e.sample_values, ', ')
                ) as examples
            FROM examples e
            CROSS JOIN totals t
            JOIN type_counts tc ON e.semantic_type_llm = tc.semantic_type_llm
            JOIN subtype_counts sc ON e.semantic_type_llm = sc.semantic_type_llm
                AND e.semantic_type_llm_subtype = sc.semantic_type_llm_subtype
            WHERE
                e.semantic_type_llm_subtype != 'Other'
                AND e.semantic_type_llm != 'Test'
                AND e.semantic_type_llm IS NOT NULL
                AND e.semantic_type_llm_subtype IS NOT NULL
            GROUP BY e.semantic_type_llm, e.semantic_type_llm_subtype, tc.type_count, sc.subtype_count, t.total_columns
            ORDER BY e.semantic_type_llm, e.semantic_type_llm_subtype
        """).df()

    # Create a markdown table
    markdown_lines = []
    markdown_lines.append("| Semantic Type | Subtype | Examples |")
    markdown_lines.append("|---------------|---------|----------|")
    for _, row in df.iterrows():
        semantic_type = row['semantic_type_llm']
        subtype = row['semantic_type_llm_subtype']
        type_pct = row['type_percentage']
        subtype_pct = row['subtype_percentage']
        # if no examples, put N/A
        if row['examples'] is not None:
            examples = "N/A"
        else:
            examples_small = [ex[:MAX_CHARS] + ("..." if len(ex) > MAX_CHARS else "") for ex in row['examples']]
            examples = "<br>".join(examples_small)
        markdown_lines.append(f"| {semantic_type} ({type_pct}%) | {subtype} ({subtype_pct}%) | {examples} |")
    markdown_content = "\n".join(markdown_lines)
    with open("semantic_type_example.md", "w") as f:
        f.write(markdown_content)




if __name__ == "__main__":
    semantic_type_percentages()
    semantic_type_example()