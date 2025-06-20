import os
import re
from dataclasses import dataclass
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from typing import List, Optional, Dict, Callable, Literal

from src.config import logger, SOURCE_CODE_FILE_EXTENSIONS, QUERIES_DIR, ONLY_SCRAPE_SELECT_QUERIES, HEADER_N_LINES
from src.sql_scraping.extract_strings import extract_strings, ExtractedString
from src.sql_scraping.string_utils import tidy_up_query, split_sql_statements

MAIN_SQL_START_WORDS = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'WITH']

# Define the schema for nested queries structure
query_schema = pa.struct([
    ('name', pa.string()),
    ('type', pa.string()),
    ('sql', pa.string()),
    ('line', pa.int64()),
    ('text_context', pa.string()),
    ('text_context_offset', pa.int64()),
])

# Define the main schema
file_result_schema = pa.struct([
    ('file_path', pa.string()),
    ('language', pa.string()),
    ('header', pa.string()),
    ('queries', pa.list_(query_schema))
])

meta_data_file_schema = pa.struct([
    ('file_path', pa.string()),
    ('file_type', pa.string()),
    ('content', pa.string())
])

repo_result_schema = pa.schema([
    ('repo_name', pa.string()),
    ('repo_url', pa.string()),
    ('file_results', pa.list_(file_result_schema)),
    ('metadata_files', pa.list_(meta_data_file_schema))
])


@dataclass
class SqlExtractionParams:
    """Helper data class for SQL extraction parameters."""
    tables: Optional[List[str]] = None
    columns: Optional[List[str]] = None
    header: Optional[str] = None
    file_filter: Optional[Callable[[str], bool]] = None


class FileAnalysisResult:
    """Data class to represent the result of a file analysis."""

    def __init__(self, file_path: str, queries: List['ExtractedQuery'], header: Optional[str]):
        self.file_path = file_path
        self.queries = queries
        self.language = file_path.split('.')[-1] if '.' in file_path else 'unknown'
        self.header = header if header else ''

    def to_dict(self) -> Dict[str, List[Dict[str, str]]]:
        """Convert the FileAnalysisResult instance to a dictionary."""
        return {
            'file_path': self.file_path,
            'language': self.language,
            'header': self.header,
            'queries': [query.to_dict() for query in self.queries],
        }

    def to_json(self) -> str:
        """Convert the FileAnalysisResult instance to a JSON string."""
        import json
        return json.dumps(self.to_dict(), indent=4)


@dataclass
class ExtractedQuery:
    """Data class to represent a SQL query."""
    text_context: str
    name: str
    type: str
    sql: str
    line: int

    def __init__(self, name: str, extracted_string: ExtractedString):
        self.name = name
        self.type = determine_query_type(extracted_string.string)
        self.sql = extracted_string.string
        self.text_context = extracted_string.text_context
        self.text_text_context_offset = extracted_string.text_context_offset
        self.line = extracted_string.line_number

    def to_dict(self) -> Dict[str, str]:
        """Convert the SqlQuery instance to a dictionary."""
        return {
            'name': self.name,
            'type': self.type,
            'sql': self.sql,
            'line': self.line,
            'text_context': self.text_context,
            'text_context_offset': self.text_text_context_offset
        }

    def to_json(self) -> str:
        """Convert the SqlQuery instance to a JSON string."""
        import json
        return json.dumps(self.to_dict(), indent=4)


def get_dir_for_url(repo_url: str) -> str:
    url_without_protocol = repo_url.replace('https://', '').replace('http://', '')
    url_without_protocol = url_without_protocol.replace('github.com/', '')
    storage_dir_name = f"{url_without_protocol.replace('/', '_')}"
    storage_dir_name = os.path.join(QUERIES_DIR, storage_dir_name)
    return storage_dir_name


META_DATA_FILE_ENDINGS = ['.json', '.yaml', '.yml', '.txt', '.md']
MetaDataFileEnding = Literal[tuple(META_DATA_FILE_ENDINGS)]  # type: ignore

MAX_METADATA_FILE_SIZE = 1 * 1024 * 1024  # 1 MB

class MetaDataFile:
    """Class to handle metadata files in a repository."""

    def __init__(self, file_path: str, file_type: MetaDataFileEnding, content: str):
        self.file_path = file_path
        self.file_type = file_type
        self.content = content

    def to_dict(self) -> Dict[str, str]:
        """Convert the MetaDataFile instance to a dictionary."""
        return {
            'file_path': self.file_path,
            'file_type': self.file_type,
            'content': self.content
        }


class RepoAnalysisResult:
    """Data class to represent the result of a repository analysis."""

    def __init__(self, repo_name: str, repo_url: str, file_results: List[FileAnalysisResult],
                 metadata_files: List[MetaDataFile]):
        self.repo_name = repo_name
        self.repo_url = repo_url
        self.file_results = file_results
        self.metadata_files = metadata_files

    def get_number_of_queries(self) -> int:
        """Get the total number of SQL queries extracted from the repository."""
        return sum(len(file_result.queries) for file_result in self.file_results)

    def to_dict(self) -> Dict[str, List[Dict[str, List[Dict[str, str]]]]]:
        """Convert the RepoAnalysisResult instance to a dictionary."""
        return {
            'repo_name': self.repo_name,
            'repo_url': self.repo_url,
            'file_results': [file_result.to_dict() for file_result in self.file_results],
            'metadata_files': [metadata_file.to_dict() for metadata_file in self.metadata_files]
        }

    def to_json(self) -> str:
        """Convert the RepoAnalysisResult instance to a JSON string."""
        import json
        return json.dumps(self.to_dict(), indent=4)

    def save(self, storage_dir: Optional[str] = None) -> None:
        # save queries to a file
        if not os.path.exists(QUERIES_DIR):
            os.makedirs(QUERIES_DIR, exist_ok=True)

        logger.info(f"Saving analysis result for {self.repo_name} to {QUERIES_DIR}.")

        if storage_dir is not None:
            storage_dir_name = storage_dir
        else:
            storage_dir_name = get_dir_for_url(self.repo_url)
            # create a dir if it does not exist
            if not os.path.exists(storage_dir_name):
                os.makedirs(storage_dir_name, exist_ok=True)

        storage_path = os.path.join(storage_dir_name, f"{self.repo_name}_analysis.parquet")

        data = [self.to_dict()]

        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df, schema=repo_result_schema)
        pq.write_table(table, storage_path)


def extract_sql_queries(file_path: str, repo_url: str,
                        params: Optional[SqlExtractionParams] = None) -> FileAnalysisResult:
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} does not exist.")
        return FileAnalysisResult(file_path, [], header=None)

    if not any(file_path.endswith(ext) for ext in SOURCE_CODE_FILE_EXTENSIONS):
        logger.warning(f"File {file_path} is not a recognized source code file.")
        return FileAnalysisResult(file_path, [], header=None)

    # Initialize parameters if not provided
    if params is None:
        params = SqlExtractionParams()

    # Read the file content
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            content = file.read()
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return FileAnalysisResult(file_path, [], header=None)

    # Extract SQL functions based on file extension

    # if file_path.endswith('.sql'):
    #     # For SQL files, extract entire SQL statements
    #     sql_queries = extract_sql_from_sql_file(content, params)
    # else:
    # For other source code files, extract SQL strings
    sql_queries = extract_sql_from_source_code(content, file_path, params)
    header = None

    if len(sql_queries) > 0:
        content_lines = content.split('\n')
        n_lines = min(len(content_lines), HEADER_N_LINES)
        header = '\n'.join(content_lines[:n_lines])  # Get the first 10 lines as header

    return FileAnalysisResult(file_path, sql_queries, header=header)


def extract_sql_from_source_code(content: str, file_path: str, params: SqlExtractionParams) -> List[ExtractedQuery]:
    """Extract SQL statements from any source code file using a unified pattern."""
    queries = []

    # if the file is a sql file, we can directly extract the SQL statements, split them by semicolon
    if file_path.endswith('.sql'):
        splitted_statements = split_sql_statements(content)
        extracted_strings: List[ExtractedString] = [
            ExtractedString(
                string=statement,
                text_context='',
                line_number=get_line_number(content, statement),
                node_content_length=len(statement)
            ) for i, statement in enumerate(splitted_statements)
        ]

    else:
        # Unified pattern: capture anything between quotes that might contain SQL
        extracted_strings: List[ExtractedString] = extract_strings(file_path)

    logger.debug(f"Extracted {len(extracted_strings)} potential SQL Queries from file {file_path}.")

    for extracted_string in extracted_strings:

        extracted_string.tidy_up()

        if len(extracted_string.string) == 0:
            continue

        if not looks_like_sql(extracted_string.string):
            continue

        if should_include_sql(extracted_string, params):
            queries.append(
                ExtractedQuery(
                    name=f"query_{len(queries) + 1}",
                    extracted_string=extracted_string,
                )
            )

    return queries


# Need to Associate atleast one valueset to the code:
def looks_like_sql(text: str) -> bool:
    # also with one whitespace behind
    main_start_words = [word + ' ' for word in MAIN_SQL_START_WORDS]
    # only allow the first word to be a main SQL command
    if not text.strip() or not any(text.strip().upper().startswith(word) for word in main_start_words):
        return False

    if ONLY_SCRAPE_SELECT_QUERIES and not text.strip().upper().startswith('SELECT'):
        return False

    sql_keywords = {
        'SELECT', 'FROM', 'WHERE', 'ALTER', 'INSERT INTO', 'INSERT OR REPLACE INTO',
        'VALUES',
        'JOIN', 'LEFT', 'RIGHT',
        'INNER', 'OUTER', 'CREATE', 'TABLE', 'DROP', 'CREATE TYPE'
                                                     'UNION', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET',
    }
    # after these keywords, there is usually a space or a parenthesis
    sql_keywords = ({kw + ' ' for kw in sql_keywords} |
                    {kw + '(' for kw in sql_keywords} |
                    {kw + ')' for kw in sql_keywords})
    text_upper = text.upper()

    # count how many SQL keywords are present in the text
    keyword_count = sum(1 for kw in sql_keywords if kw in text_upper)
    # if there are at least 2 SQL keywords, it looks like SQL
    return keyword_count >= 2


def determine_query_type(sql_string: str) -> str:
    # check which of the main SQL keywords is the first one in the string
    for word in MAIN_SQL_START_WORDS:
        if sql_string.upper().startswith(word):
            return word.upper()

    # If no keyword matches, return 'unknown'
    return 'UNKNOWN'


def should_include_sql(sql_string: ExtractedString, params: SqlExtractionParams) -> bool:
    """Check if a SQL string should be included based on the parameters."""
    # If no tables or columns are specified, include all SQL
    if not params.tables and not params.columns:
        return True

    # Convert to uppercase for case-insensitive comparison
    sql_upper = sql_string.string.upper()

    # Check if any of the specified tables are mentioned
    if params.tables:
        table_mentioned = any(table.upper() in sql_upper for table in params.tables)
        if not table_mentioned:
            return False

    # Check if any of the specified columns are mentioned
    if params.columns:
        column_mentioned = any(column.upper() in sql_upper for column in params.columns)
        if not column_mentioned:
            return False

    return True


def get_line_number(content: str, search_text: str) -> int:
    """Get the line number where a text appears in the content."""
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if search_text in line:
            return i + 1
    return 1  # Default to line 1 if not found


def get_repo_files(repo_dir: str) -> List[str]:
    """Recursively get a list of source code files in the repository directory."""
    code_files = []

    if not os.path.exists(repo_dir):
        logger.error(f"Repository directory {repo_dir} does not exist.")
        return []

    for root, _, files in os.walk(repo_dir):
        for file in files:
            if any(file.endswith(ext) for ext in SOURCE_CODE_FILE_EXTENSIONS):
                full_path = os.path.join(root, file)
                code_files.append(full_path)

    return code_files


def extract_sql_from_repo(repo_dir: str, repo_url: str, params: Optional[SqlExtractionParams] = None) -> List[
    FileAnalysisResult]:
    results = []

    code_files = get_repo_files(repo_dir)

    if params and params.file_filter:
        code_files = [file for file in code_files if params.file_filter(file)]

    for file_path in code_files:
        file_result = extract_sql_queries(file_path, repo_url, params)
        results.append(file_result)

    return results


if __name__ == "__main__":
    # Example usage
    import sys

    if len(sys.argv) < 2:
        print("Usage: python extract_sql.py <dir_path> [table1,table2,...] [column1,column2,...]")
        sys.exit(1)

    dir_path = sys.argv[1]

    # Parse optional tables and columns
    tables = sys.argv[2].split(',') if len(sys.argv) > 2 else None
    columns = sys.argv[3].split(',') if len(sys.argv) > 3 else None

    params = SqlExtractionParams(tables=tables, columns=columns)

    extracted_queries = extract_sql_from_repo(dir_path, params)
