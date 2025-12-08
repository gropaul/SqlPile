from typing import List, Optional, Callable
import duckdb
from tqdm import tqdm

from src.config import (
    get_con,
    COLUMNS_TABLE_NAME,
    TABLE_VALUES_COUNT_TABLE_NAME,
    COLUMN_VALUES_TABLE_NAME,
    MAX_VALUES_TO_SAVE_PER_COLUMN
)
from src.sql_analysis.add_3rd_party.utils import get_benchmark_repo_name, get_benchmark_repo_url
from src.sql_analysis.utils.delete_data import delete_repo
from src.sql_analysis.tools.sql_types import unify_type
from src.sql_analysis.tools.semantic_type import get_column_semantic_type


class DatasetAdapter:
    """
    Generic adapter for importing external datasets (Kaggle, HuggingFace, etc.)
    into the main SqlPile database.
    """

    def __init__(
        self,
        source_db_path: str,
        source_db_alias: str,
        repo_prefix: str,
        clean_name_fn: Optional[Callable[[str], str]] = None,
        top_n_downloaded_repos: Optional[int] = None
    ):
        """
        Initialize the dataset adapter.

        Args:
            source_db_path: Path to the source database (e.g., kaggle_data.duckdb)
            source_db_alias: Alias to attach the source database as (e.g., 'kaggle_data')
            repo_prefix: Prefix for repository names (e.g., 'kaggle', 'huggingface')
            clean_name_fn: Optional function to clean schema/table names
        """
        self.source_db_path = source_db_path
        self.source_db_alias = source_db_alias
        self.repo_prefix = repo_prefix
        self.clean_name_fn = clean_name_fn or (lambda x: x)
        self.con = None
        self.top_n_downloaded_repos = top_n_downloaded_repos

    def connect(self):
        """Connect to the main database and attach the source database."""
        self.con = get_con()
        self.con.execute(f"ATTACH DATABASE '{self.source_db_path}' AS {self.source_db_alias}")

    def close(self):
        """Close the database connection."""
        if self.con:
            self.con.close()

    def add_schema_as_repo(self, schema_name: str) -> Optional[int]:
        """
        Add a schema/dataset as a repository in the main database.

        Args:
            schema_name: Name of the schema in the source database

        Returns:
            The repo_id of the newly created repository, or None if failed
        """
        # Clean the schema name
        schema_name_clean = self.clean_name_fn(schema_name)
        schema_name_clean = f'{self.repo_prefix}-{schema_name_clean}'

        repo_name = get_benchmark_repo_name(schema_name_clean)

        # Check if the repo already exists
        existing_repo = self.con.execute(
            "SELECT id FROM repos WHERE repo_name = ?",
            (repo_name,)
        ).fetchone()

        if existing_repo is not None:
            existing_id = existing_repo[0]
            delete_repo(self.con, existing_id)

        # Get next repo ID
        max_repo_id = self.con.execute("SELECT MAX(id) FROM repos").fetchone()[0]
        repo_id = max_repo_id + 1 if max_repo_id is not None else 0

        # Insert the repository
        repo_url = get_benchmark_repo_url(schema_name_clean)
        self.con.execute(
            "INSERT INTO repos (id, repo_name, repo_url) VALUES (?, ?, ?)",
            (repo_id, repo_name, repo_url)
        )

        return repo_id

    def add_table_to_db(
        self,
        repo_id: int,
        schema_name: str,
        table_name: str,
        columns: List[dict]
    ):
        """
        Add a table and its columns to the main database.

        Args:
            repo_id: ID of the repository this table belongs to
            schema_name: Schema name in the source database
            table_name: Name of the table
            columns: List of column dictionaries with keys: column_name, data_type, ordinal_position
        """
        # Clean table name
        table_name_clean = self.clean_name_fn(table_name)

        # Check if the table already exists
        existing_table = self.con.execute(
            "SELECT id FROM tables WHERE repo_id = ? AND table_name_clean = ?",
            (repo_id, table_name_clean)
        ).fetchone()

        if existing_table is not None:
            return

        # Get next table ID
        table_id = self.con.execute("SELECT MAX(id) FROM tables").fetchone()[0]
        table_id = table_id + 1 if table_id is not None else 0

        # Insert the table
        self.con.execute(
            "INSERT INTO tables (id, repo_id, table_name, table_name_clean, file_url) VALUES (?, ?, ?, ?, ?)",
            (table_id, repo_id, table_name, table_name_clean, None)
        )

        # Get row count
        try:
            row_count = self.con.execute(
                f'SELECT COUNT(*) FROM "{self.source_db_alias}"."{schema_name}"."{table_name}"'
            ).fetchone()[0]

            # Insert row count
            self.con.execute(
                f"INSERT INTO {TABLE_VALUES_COUNT_TABLE_NAME} (table_id, count) VALUES (?, ?)",
                (table_id, row_count)
            )
        except Exception as e:
            print(f"Error getting row count for table {table_name}: {str(e)}")
            row_count = 0

        # Add columns
        self._add_columns(table_id, schema_name, table_name, columns)

    def _add_columns(
        self,
        table_id: int,
        schema_name: str,
        table_name: str,
        columns: List[dict]
    ):
        """
        Add columns to a table.

        Args:
            table_id: ID of the table
            schema_name: Schema name in the source database
            table_name: Name of the table
            columns: List of column dictionaries
        """
        for col in columns:
            column_id = self.con.execute("SELECT MAX(id) FROM columns").fetchone()[0]
            column_id = column_id + 1 if column_id is not None else 0

            column_name = col['column_name']
            column_table_index = col['ordinal_position']
            column_type_original = col['data_type']

            column_type, base_type = unify_type(column_type_original)

            # Insert column
            self.con.execute(
                f"""
                INSERT INTO {COLUMNS_TABLE_NAME} (
                    id, table_id, column_name, column_table_index, column_type, column_base_type,
                    column_type_original, semantic_type, is_unique, is_nullable,
                    is_indexed, is_primary_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    column_id, table_id, column_name, column_table_index, column_type, base_type,
                    column_type_original, '', False, True, False, False
                )
            )

            # If it's a text column, add sample values
            if base_type == "Text":
                self._insert_column_values(column_id, schema_name, table_name, column_name)

    def _insert_column_values(
        self,
        column_id: int,
        schema_name: str,
        table_name: str,
        column_name: str
    ):
        """
        Insert sample values for a text column.

        Args:
            column_id: ID of the column
            schema_name: Schema name in the source database
            table_name: Name of the table
            column_name: Name of the column
        """
        insert_query = f"""
            INSERT INTO {COLUMN_VALUES_TABLE_NAME}
            SELECT {column_id}, "{column_name}" AS value
            FROM "{self.source_db_alias}"."{schema_name}"."{table_name}"
            USING SAMPLE {MAX_VALUES_TO_SAVE_PER_COLUMN}
        """

        try:
            self.con.execute(insert_query)
        except Exception as e:
            print(f"Error inserting values for column {column_name} in table {table_name}: {str(e)}")

    def add_schema(self, schema_name: str):
        """
        Add a complete schema with all its tables and columns.

        Args:
            schema_name: Name of the schema in the source database
        """
        repo_id = self.add_schema_as_repo(schema_name)

        if repo_id is None:
            print(f"Failed to create repository for schema {schema_name}")
            return

        # Get all tables and their columns
        query = f"""
            SELECT table_name, list({{
                column_name: column_name,
                data_type: data_type,
                ordinal_position: ordinal_position
            }}) AS table_columns
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
            GROUP BY table_name
            ORDER BY table_name
        """

        tables = self.con.execute(query).fetchall()

        for table_name, columns in tables:
            self.add_table_to_db(repo_id, schema_name, table_name, columns)

    def get_schemas(self) -> List[str]:
        """
        Get all schemas in the source database.

        Returns:
            List of schema names
        """
        schemas = self.con.execute(f"""
            SELECT DISTINCT table_schema
            FROM information_schema.tables
            WHERE table_catalog = '{self.source_db_alias}'
            GROUP BY table_schema
            ORDER BY table_schema
        """).fetchall()

        return [schema[0] for schema in schemas]

    def import_all_schemas(self):
        """
        Import all schemas from the source database into the main database.
        """
        schemas = self.get_schemas()
        print(f"Found {len(schemas)} schemas to process.")

        for schema_name in tqdm(schemas):
            self.add_schema(schema_name)

    def delete_repos_by_prefix(self):
        """
        Delete all repositories that match the repo prefix.
        """
        repos = self.con.execute(
            f"SELECT id, repo_name FROM repos WHERE repo_name LIKE '3rd-party-{self.repo_prefix}-%'"
        ).fetchall()

        print(f"Found {len(repos)} {self.repo_prefix} repos to delete.")

        for repo_id, repo_name in repos:
            print(f"Deleting repo {repo_name} with id {repo_id}")
            delete_repo(self.con, repo_id)
