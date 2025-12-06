import os.path

from docs.gen.utils import format_number, join_list, format_latex_string
from src.config import LATEX_GEN_DIR, get_con, QUERIES_TABLE_NAME
import duckdb

# the section name is the filename without the extension
SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

text = """
Schemapile~\cite{dohmen_schemapile_2024} contains used the the list of GitHub repositories that are know to contain 
SQL code together with data on tables and some values for each table. To use this dataset to analyze the use of text columns, 
we also need to extract `SELECT` queries. Therefore, for each repository in Schemapile, we collected all SQL queries 
from both `.sql` files and SQL strings in application code files. 

In total we retrieved {number_of_queries} queries from {number_of_repos} repositories, 
with {number_of_queries_per_type}. By parsing the `CREATE` SQL statements, we could extract 
{number_of_tables} distinct tables with {number_of_columns} columns. In order to execute the queries 
later in DuckDB, we unified the system specific column types into abstract types like `INTEGER`, `FLOAT`, 
`TEXT` or `DATETIME` to then later map them to the DuckDB types. 

In order to analyze the collected `SELECT` statements, we used DuckDB~\cite{raasveldt_duckdb_2019} to create a local database for each repository.
First, we created and populated the tables using the `CREATE TABLE` statements and `INSERT` statements found in the code.
Then, we executed the `SELECT` statements to retrieve query plans using DuckDB's `EXPLAIN` functionality.

Since queries are rarely written as static strings in the code, we first had to preprocess them by 
replacing variables and applying minor transformations to make them executable in DuckDB. This way, we where 
able to execute {select_success_percentage} of the `SELECT` statements resulting in a 
total number of {select_success_n} statements we could retrieve query plans for. 

Due to errors in the extracted SQL and differences in the features, data types and SQL dialects across systems, we could only execute {percentage_create_table} 
of the `CREATE TABLE`, {percentage_create_view} of the `CREATE VIEW` and {percentage_insert_statements} of the `INSERT` statements. 
From this, we could populate {tables_with_data} tables with at least one row, 
with a median of {median_n_rows} rows and a mean of {mean_n_rows} rows per table.

To then be able to track the usage of columns through the operators, we implemented a special 
explain function\\footnote{{See \\url{{https://todo}}}} that returns not only information on 
the operators used to execute the queries but also the details on the expressions these use. 
"""


def main():
    con = get_con(read_only=True)

    con.execute("""
                CREATE OR REPLACE TEMP VIEW external_repos AS
                SELECT id
                FROM repos
                WHERE lower(get_repo_origin(repo_url)) != 'DBPile'
                """
                )
    con.execute("""
                CREATE OR REPLACE TEMP VIEW external_tables AS
                SELECT id
                FROM tables
                WHERE repo_id IN (SELECT id FROM external_repos)
                """)
    con.execute("""
                CREATE OR REPLACE TEMP TABLE external_columns AS
                SELECT id
                FROM columns
                WHERE table_id IN (SELECT id FROM external_tables)
                """)
    con.execute(f"""
                CREATE OR REPLACE TEMP TABLE external_queries AS
                SELECT id
                FROM {QUERIES_TABLE_NAME}
                WHERE repo_id IN (SELECT id FROM external_repos)
                """)

    number_of_tables = con.execute(
        "SELECT COUNT(*) FROM tables WHERE id NOT IN (SELECT ID FROM external_tables)").fetchone()[0]
    number_of_columns = con.execute(
        "SELECT COUNT(*) FROM columns WHERE id NOT IN (SELECT ID FROM external_columns)").fetchone()[0]
    number_of_queries = con.execute(
        "SELECT COUNT(*) FROM queries WHERE id NOT IN (SELECT ID FROM external_queries)").fetchone()[0]
    number_of_repos = con.execute(
        "SELECT COUNT(DISTINCT repo_id) FROM queries WHERE id NOT IN (SELECT ID FROM external_queries)").fetchone()[0]

    print(f"Number of tables: {number_of_tables}")
    print(f"Number of columns: {number_of_columns}")
    print(f"Number of queries: {number_of_queries}")
    print(f"Number of repos: {number_of_repos}")

    allowed = ["SELECT", "INSERT", "CREATE"]

    number_of_queries_per_type = con.execute(f"""
        with cnts AS (
            SELECT repo_id, type, COUNT(sql) as repo_cnt
            FROM queries
            WHERE 
                type in {allowed}
                AND id NOT IN (SELECT ID FROM external_queries)
            GROUP BY repo_id, type
        )
        SELECT type, SUM(repo_cnt) as sum
        FROM cnts
        GROUP BY type
        ORDER BY sum DESC
    """).fetchall()

    print(f"Number of queries per type: {number_of_queries_per_type}")

    percentages_per_type = []
    for (index, (query_type, count)) in enumerate(number_of_queries_per_type):
        percentage = (count / number_of_queries) * 100
    number_formatted = format_number(count)
    # text_item = f"{number_formatted} `{query_type}` ({percentage:.2f}%)"
    text_item = f"{number_formatted} `{query_type}`"
    if index == 0:
        text_item = f"{number_formatted} distinct `{query_type}`"
    percentages_per_type.append(text_item)

    number_of_queries_per_type_str = join_list(percentages_per_type, final_word="statements")

    # *** CREATE STATEMENTS ***
    number_of_create_statements = con.execute(f"""
        WITH create_queries as (
            SELECT sql
            FROM queries
            WHERE 
                type = 'CREATE'
                AND id NOT IN (SELECT ID FROM external_queries)
        )
        SELECT (get_table_name_udf(sql) is Null) AS has_table_name, COUNT(*)
        FROM create_queries
        GROUP BY has_table_name ORDER BY has_table_name
      """).fetchall()

    number_of_create_view_statements = con.execute(f"""
        WITH create_queries as (
            SELECT sql
            FROM queries
            WHERE 
                type = 'CREATE'
                AND id NOT IN (SELECT ID FROM external_queries)
        )
        SELECT COUNT(*)
        FROM create_queries
        WHERE is_create_view_udf(sql)
        """).fetchone()[0]

    number_of_view_errors = con.execute(f"SELECT COUNT(*) FROM queries_error_create_view WHERE query_id NOT IN (SELECT id FROM external_queries)").fetchone()[0]
    percentage_view_queries = ((
                                       number_of_create_view_statements - number_of_view_errors) / number_of_create_view_statements) * 100
    percentage_view_queries = f"{percentage_view_queries:.2f}%"

    parsable, not_parsable = number_of_create_statements[0][1], number_of_create_statements[1][1]
    parsable -= number_of_create_view_statements  # remove the create view statements from the parsable count

    second_parsing_error_count = con.execute("SELECT COUNT(*) FROM queries_parsing_error WHERE query_id NOT IN (SELECT id FROM external_queries)").fetchone()[0]
    parsable -= second_parsing_error_count
    not_parsable += second_parsing_error_count
    percentage_create_table = (parsable / (parsable + not_parsable)) * 100
    percentage_create_table = f"{percentage_create_table:.2f}%"

    print(f"Number of CREATE statements: {parsable} parsable, {not_parsable} not parsable")

    # *** INSERT STATEMENTS ***
    number_of_inserts = con.execute("SELECT COUNT(*) FROM queries WHERE type = 'INSERT' AND repo_id NOT IN (SELECT id FROM external_repos)").fetchone()[0]
    number_of_failed_inserts = con.execute("SELECT COUNT(*) FROM queries_error_insert WHERE query_id NOT IN (SELECT id FROM external_queries)").fetchone()[0]

    percentage_insert_statements = (number_of_inserts - number_of_failed_inserts) / number_of_inserts * 100
    percentage_insert_statements = f"{percentage_insert_statements:.2f}%"

    print(f"Number of INSERT statements: {number_of_inserts} total, {number_of_failed_inserts} failed")

    # *** Number of tables with values ***
    table_with_more_then_100_rows, tables_with_data, median_n_rows, mean_n_rows = con.execute(f"""
        SELECT SUM(count > 100) AS tables_with_more_then_100_rows, SUM(count > 0) AS tables_with_data, MEDIAN(count) AS median_n_rows, AVG(count) AS mean_n_rows
        FROM table_values_count
        WHERE table_values_count.table_id NOT IN (SELECT ID FROM external_tables)
        """).fetchone()

    print(f"Tables with more then 100 rows: {table_with_more_then_100_rows}")

    # queries_count
    select_success_n = con.execute("SELECT COUNT(*) FROM queries_executable WHERE query_id NOT IN (SELECT id FROM external_queries)").fetchone()[0]
    select_error_n = con.execute("SELECT COUNT(*) FROM queries_error_select WHERE query_id NOT IN (SELECT id FROM external_queries)").fetchone()[0]

    select_success_percentage = (select_success_n / (select_success_n + select_error_n)) * 100
    select_success_percentage = f"{select_success_percentage:.2f}%"

    description = text.format(
        number_of_tables=format_number(number_of_tables),
        number_of_columns=format_number(number_of_columns),
        number_of_queries=format_number(number_of_queries),
        number_of_repos=format_number(number_of_repos),
        number_of_queries_per_type=number_of_queries_per_type_str,
        percentage_create_table=percentage_create_table,
        percentage_insert_statements=percentage_insert_statements,
        percentage_create_view=percentage_view_queries,
        tables_with_data=format_number(tables_with_data),
        median_n_rows=format_number(median_n_rows),
        mean_n_rows=format_number(mean_n_rows),
        select_success_percentage=select_success_percentage,
        select_success_n=format_number(select_success_n)
    )

    description = format_latex_string(description)

    path = os.path.join(LATEX_GEN_DIR, SECTION_NAME)
    with open(path, "w") as f:
        f.write(description)

    print(f"Dataset description written to {path}")
    return description


if __name__ == "__main__":
    print(main())
