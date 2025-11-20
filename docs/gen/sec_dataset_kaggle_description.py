import os.path

from docs.gen.utils import format_number, join_list_with_and, format_latex_string
from src.config import LATEX_GEN_DIR, get_con
import duckdb

# the section name is the filename without the extension
SECTION_NAME = __file__.split("/")[-1].replace(".py", ".tex")

text = """

"""


def generate_dataset_description():
    con = get_con(read_only=True)
    kaggle_con = duckdb.connect(
        os.path.join(LATEX_GEN_DIR, "kaggle_datasets.duckdb"), read_only=True
    )


if __name__ == "__main__":
    print(generate_dataset_description())
