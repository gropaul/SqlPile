import os

import pandas as pd

from src.config import get_con, LATEX_ASSETS_DIR

MAX_CHARS = 200
MAX_EXAMPLES_PER_TYPE = 100
MAX_VALUES_PER_EXAMPLE = 3


# map from type to subtype to example value
EXAMPLES = {
    "Category": {
        "Boolean": 'hall-of-fame.inducted: "Y", "N"',
        "N/A": 'matches.winner: "DRAW", "AWAY_TEAM", "HOME_TEAM"',
        "Other": 'player.bats: "B", "L", "R"'
    },
    "Entity": {
        "Location": 'otp.station: "Airport Termina..."',
        "N/A": 'StateNames.State: "AL", "AK", "AR"',
        "Organization": 'game.team_home: "Indianapolis Jets"',
        "Other": 'date_dim.day_name: "Tuesday"',
        "Person": 'player.name_last: "Bockman"'
    },
    "FullText": {
        "Formatted": 'title.title: "Fish Follies"',
        "N/A": 'AI_scopus.Abstract: "Biclustering solut..."',
        "Unformatted": 'reviews.message: "Material rece..."'
    },
    "Identifier": {
        "Generic": 'items.id: "5c582483eb0e06d5..."',
        "N/A": 'boarding_passes.ticket_no: "0005433722199"',
        "Path": 'users.Url: "https://www.bruc..."',
        "Semantic": 'fielding.team_id: "WS4", "NH1"'
    },
    "Numeric": {
        "DateTime": 'orders.date: "2017-09-14 18:33:42"',
        "N/A": 'AI_scopus.Year: "2001"',
        "Other": 'fielding.sb: "52.0"'
    },
    "Other": {
        "N/A": 'Users.password: "1309340755y"'
    },
    "Structured": {
        "CSV": 'Events.Guests: "John Doe, ..."',
        "JSON": 'round.hist: "{1:{\'RoundWinner\': ..."',
        "List": 'Post.Tags: "<mysql><innodb>..."',
        "Other": 'Match.shotoff: "<shotoff><value>..."',
    },
    "Test": {
        "N/A": 'users.name: "test", "foo"'
    }
}



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
                    CASE 
                        WHEN 100.0 * a.type_count / t.total_count > 10
                            THEN ROUND(100.0 * a.type_count / t.total_count, 0)
                        ELSE 
                            ROUND(100.0 * a.type_count / t.total_count, 1)
                    END AS type_percentage

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
    df["AVG"] = df[datasets].mean(axis=1)

    datasets = datasets + ["AVG"]

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

    df = df.sort_values(by=["Semantic Type", "AVG"], ascending=[True, False]).reset_index(drop=True)

    # format all the datasets columns as percentages with 1 decimal place
    for col in datasets:
        def fmt(x):
            if x is None or pd.isna(x) or x == 0:
                return "-"
            if x >= 10:
                return f"{round(x, 0):.0f}\\%"
            else:
                return f"{round(x, 1):.1f}\\%"

        df[col] = df[col].apply(fmt)

    # fill in the example column based on the EXAMPLES dictionary
    def get_example(row):
        sem_type = row["Semantic Type"]
        subtype = row["Subtype"]
        if sem_type in EXAMPLES and subtype in EXAMPLES[sem_type] and subtype != 'N/A':
            text = EXAMPLES[sem_type][subtype].replace("_", "\\_").replace("%", "\\%").replace("'", "\\textquotesingle ").replace('{', "\\{").replace('}', "\\}")
            return f"\\small\\texttt{{{text}}}"
        print(f"No example found for type {sem_type} and subtype {subtype}")
        return ""

    df.insert(2, "Examples", "")
    df["Examples"] = df.apply(get_example, axis=1)

    # for all columns where there is not an 'All' or 'N/A', make the type empty
    def format_subtype(row):
        row_idx = row.name
        if row["Subtype"] not in ["All", "N/A"]:
            return ""
        return f"HEAD{row_idx}_: {row['Semantic Type']}"

    df["Semantic Type"] = df.apply(format_subtype, axis=1)

    # rename Stack Overflow to SO
    df = df.rename(columns={"Stack Overflow": "SO"})
    df = df.rename(columns={"HuggingFace": "HF"})
    df = df.rename(columns={"Semantic Type": "Type"})

    # add an empty example column after the Subtype column



    latex_table = df.to_latex(index=False, float_format="%.2f", na_rep="-")
    # bold all column names
    for col in df.columns:
        latex_table = latex_table.replace(col, f"\\small{{\\textbf{{{col}}}}}", 1)

    latex_lines = latex_table.splitlines()
    # for each line that starts with HEAD, wrap the whole row with a bold command and extract the type name
    formatted_lines = []
    for line in latex_lines:
        if line.strip().startswith("HEAD"):
            # extract the type name
            line_without_head = line.replace("HEAD", "")
            row_index, rest_of_line = line_without_head.split("_:", 1)
            # wrap every cell in bold
            line_parts = rest_of_line.split("&")
            line_bold_parts = [f"\\textbf{{{part.strip()}}}" for part in line_parts]
            line_bold = " & ".join(line_bold_parts)

            # replace the \\} at the end with }\\
            if line_bold.strip().endswith("\\\\}"):
                line_bold = line_bold.strip()[:-3].strip() + "} \\\\"

            # if this is not the first line, add \addlinespace[0.6em] before the line
            if len(formatted_lines) > 0:
                line_bold = "\\addlinespace[0.6em] " + line_bold

            formatted_lines.append(line_bold)

        else:
            formatted_lines.append(f"{line}")

    latex_content = "\n".join(formatted_lines)
    latex_content_small = f"\\begin{{small}}\n{latex_content}\n\\end{{small}}"
    # save this as a latex table
    with open(path, "w") as f:
        f.write(latex_content_small)


def semantic_type_example():
    con = get_con(read_only=True)



    # First, get all combinations with their examples and percentages
    df = con.sql(f"""
            WITH ranked_columns AS (
                SELECT
                    semantic_type_llm,
                    ifnull(semantic_type_llm_subtype, 'N/A') as semantic_type_llm_subtype,
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
                WHERE tvc.count > 1000
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
                    ifnull(semantic_type_llm_subtype, 'N/A') as semantic_type_llm_subtype,
                    COUNT(*) as subtype_count
                FROM columns
                WHERE semantic_type_llm IS NOT NULL
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
                e.semantic_type_llm IS NOT NULL
            GROUP BY e.semantic_type_llm, e.semantic_type_llm_subtype, tc.type_count, sc.subtype_count, t.total_columns
            ORDER BY e.semantic_type_llm, e.semantic_type_llm_subtype
        """).df()

    # save df as csv for debugging
    df.to_csv("semantic_type_examples_debug.csv", index=False)

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
        if row['examples'] is None:
            examples = "N/A"
        else:
            examples_small = [ex[:MAX_CHARS] + ("..." if len(ex) > MAX_CHARS else "") for ex in row['examples']]
            examples = "<br>".join(examples_small)
        markdown_lines.append(f"| {semantic_type} ({type_pct}%) | {subtype} ({subtype_pct}%) | {examples} |")
    markdown_content = "\n".join(markdown_lines)
    with open("semantic_type_example.md", "w") as f:
        f.write(markdown_content)


def main():
    semantic_type_percentages()
    semantic_type_example()



if __name__ == "__main__":
    main()