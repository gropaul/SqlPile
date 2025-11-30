import os.path

from docs.gen.utils import format_number, join_list_with_and, format_latex_string
from src.config import LATEX_GEN_DIR, get_con, HUGGINFACE_DATA_DB_PATH, KAGGLE_DATA_DB_PATH, KAGGLE_DATASETS_DB_PATH, \
    MAX_VALUES_TO_DOWNLOAD
import duckdb

from src.data_analysis.storage.huggingface.download_data import format_bytes

# the section name is the filename without the extension
SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

text = """
As described before, SchemaPile does not contain enough values per table to meaningful analyze properties like 
value distribution and compression rations. Therefore, we also analyzed to public data repository like Kaggle
and HuggingFace, which are mainly used for ML and AI applications.

For our dataset, we looked at datasets on Kaggle and Huggingface provided as Parquet files. 
From Kaggle, we retrieved {kaggle_datasets_count} datasets with {kaggle_tables_count} tables, 
averaging {avg_kaggle_rows_per_table} rows per table. Huggingface provides {huggingface_datasets_count} 
datasets with {huggingface_tables_count} tables and an average of {avg_huggingface_rows_per_table} rows per table.
For both sources, parquet datasets typically contain only a 
single table. For kaggle, there are more tables per dataset as we downloaded {kaggle_sqlite_datasets_count} SQLite-based 
Kaggle datasets in addition to the parquet based datasets.

Huggingface and Kaggle use very wide tables, with about {huggingface_avg_columns_per_table} 
and {kaggle_avg_columns_per_table} columns per table, respectively. In contrast, TPC-[H, DS] 
tables average only {tpc_avg_columns_per_table} columns, and DBPile tables about 
{dbpile_avg_columns_per_table}. For all tables, we limited the extracted data to a maximum of 
{max_values_to_download} rows in order to analyze value distributions and compression 
ratios without excessively increasing the overall dataset size. We store these subsets in one 
duckdb database with different schemas per dataset, leading to a total database size of about
{kaggle_data_size} for Kaggle and {huggingface_data_size} for Huggingface.
"""


def generate_dataset_description():
    con = get_con(read_only=True)
    hf_con = duckdb.connect(HUGGINFACE_DATA_DB_PATH, read_only=True)
    kaggle_con = duckdb.connect(KAGGLE_DATA_DB_PATH, read_only=True)
    kaggle_info_con = duckdb.connect(KAGGLE_DATASETS_DB_PATH, read_only=True)

    kaggle_datasets_count = \
        kaggle_con.execute("SELECT COUNT(DISTINCT table_schema) FROM information_schema.tables").fetchone()[0]
    kaggle_tables_count = kaggle_con.execute("SELECT COUNT(*) FROM information_schema.tables").fetchone()[0]
    kaggle_columns_count = kaggle_con.execute("SELECT COUNT(*) FROM information_schema.columns").fetchone()[0]
    kaggle_avg_columns_per_table = kaggle_columns_count / kaggle_tables_count

    kaggle_sqlite_datasets_count = kaggle_info_con.execute(
        " SELECT COUNT(DISTINCT dataset_ref) FROM kaggle_dataset_files WHERE file_name LIKE '%.sqlite';").fetchone()[0]

    huggingface_datasets_count = \
        hf_con.execute("SELECT COUNT(DISTINCT table_schema) FROM information_schema.tables").fetchone()[0]
    huggingface_tables_count = hf_con.execute("SELECT COUNT(*) FROM information_schema.tables").fetchone()[0]
    huggingface_columns_count = hf_con.execute("SELECT COUNT(*) FROM information_schema.columns").fetchone()[0]
    huggingface_avg_columns_per_table = huggingface_columns_count / huggingface_tables_count

    hf_tables = hf_con.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall()
    hf_rows_per_table = []
    for table_schema, table_name in hf_tables:
        row_count = hf_con.execute(f'SELECT COUNT(*) FROM "{table_schema}"."{table_name}"').fetchone()[0]
        hf_rows_per_table.append(row_count)

    kaggle_tables = kaggle_con.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall()
    kaggle_rows_per_table = []
    for table_schema, table_name in kaggle_tables:
        row_count = kaggle_con.execute(f'SELECT COUNT(*) FROM "{table_schema}"."{table_name}"').fetchone()[0]
        kaggle_rows_per_table.append(row_count)

    avg_kaggle_rows_per_table = sum(kaggle_rows_per_table) / len(kaggle_rows_per_table) if kaggle_rows_per_table else 0
    avg_hf_rows_per_table = sum(hf_rows_per_table) / len(hf_rows_per_table) if hf_rows_per_table else 0

    db_pile_n_tables = con.execute(
        "SELECT COUNT(*) FROM tables JOIN repos ON repos.id = tables.repo_id WHERE '3rd-party-' not in repo_url;").fetchone()[
        0]
    db_pile_n_columns = con.execute(
        "SELECT COUNT(*) FROM columns JOIN tables ON tables.id = columns.table_id JOIN repos ON repos.id = tables.repo_id WHERE '3rd-party-' not in repo_url;").fetchone()[
        0]
    dbpile_avg_columns_per_table = db_pile_n_columns / db_pile_n_tables

    tpc_n_tables = con.execute(
        "SELECT COUNT(*) FROM tables JOIN repos ON repos.id = tables.repo_id WHERE '3rd-party-tpc-' in repo_url;").fetchone()[
        0]
    tpc_n_columns = con.execute(
        "SELECT COUNT(*) FROM columns JOIN tables ON tables.id = columns.table_id JOIN repos ON repos.id = tables.repo_id WHERE '3rd-party-tpc-' in repo_url;").fetchone()[
        0]
    tpc_avg_columns_per_table = tpc_n_columns / tpc_n_tables

    kaggle_size = os.path.getsize(KAGGLE_DATA_DB_PATH)
    huggingface_size = os.path.getsize(HUGGINFACE_DATA_DB_PATH)

    filled_text = text.format(
        kaggle_datasets_count=format_number(kaggle_datasets_count),
        kaggle_tables_count=format_number(kaggle_tables_count),
        avg_kaggle_rows_per_table=format_number(int(avg_kaggle_rows_per_table)),
        avg_huggingface_rows_per_table=format_number(int(avg_hf_rows_per_table)),
        huggingface_datasets_count=format_number(huggingface_datasets_count),
        huggingface_tables_count=format_number(huggingface_tables_count),
        kaggle_sqlite_datasets_count=format_number(kaggle_sqlite_datasets_count),
        huggingface_avg_columns_per_table=round(huggingface_avg_columns_per_table, 1),
        kaggle_avg_columns_per_table=round(kaggle_avg_columns_per_table, 1),
        tpc_avg_columns_per_table=round(tpc_avg_columns_per_table, 1),
        dbpile_avg_columns_per_table=round(dbpile_avg_columns_per_table, 1),
        max_values_to_download=format_number(MAX_VALUES_TO_DOWNLOAD),
        kaggle_data_size=format_bytes(kaggle_size),
        huggingface_data_size=format_bytes(huggingface_size)
    )

    output_path = os.path.join(LATEX_GEN_DIR, SECTION_NAME)
    with open(output_path, "w") as f:
        f.write(format_latex_string(filled_text))
    return filled_text


if __name__ == "__main__":
    print(generate_dataset_description())
