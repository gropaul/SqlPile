import os
import re
from dataclasses import dataclass
from typing import List, Optional, Dict

from config import logger, SOURCE_CODE_FILE_EXTENSIONS, QUERIES_DIR
from extract_strings import extract_strings
from string_utils import tidy_up_string


@dataclass
class SqlExtractionParams:
    """Helper data class for SQL extraction parameters."""
    tables: Optional[List[str]] = None
    columns: Optional[List[str]] = None


class FileAnalysisResult:
    """Data class to represent the result of a file analysis."""

    def __init__(self, repo_url: str, file_path: str, queries: List['SqlQuery']):
        self.repo_url = repo_url
        self.file_path = file_path
        self.queries = queries
        self.language = file_path.split('.')[-1] if '.' in file_path else 'unknown'

    def to_dict(self) -> Dict[str, List[Dict[str, str]]]:
        """Convert the FileAnalysisResult instance to a dictionary."""
        return {
            'repo_url': self.repo_url,
            'file_path': self.file_path,
            'queries': [query.to_dict() for query in self.queries],
            'language': self.language
        }

    def to_json(self) -> str:
        """Convert the FileAnalysisResult instance to a JSON string."""
        import json
        return json.dumps(self.to_dict(), indent=4)


@dataclass
class SqlQuery:
    """Data class to represent a SQL query."""
    name: str
    type: str
    sql: str
    line: int

    def to_dict(self) -> Dict[str, str]:
        """Convert the SqlQuery instance to a dictionary."""
        return {
            'name': self.name,
            'type': self.type,
            'sql': self.sql,
            'line': self.line
        }

    def to_json(self) -> str:
        """Convert the SqlQuery instance to a JSON string."""
        import json
        return json.dumps(self.to_dict(), indent=4)


class RepoAnalysisResult:
    """Data class to represent the result of a repository analysis."""

    def __init__(self, repo_name: str, repo_url: str, file_results: List[FileAnalysisResult]):
        self.repo_name = repo_name
        self.repo_url = repo_url
        self.file_results = file_results

    def get_number_of_queries(self) -> int:
        """Get the total number of SQL queries extracted from the repository."""
        return sum(len(file_result.queries) for file_result in self.file_results)

    def to_dict(self) -> Dict[str, List[Dict[str, List[Dict[str, str]]]]]:
        """Convert the RepoAnalysisResult instance to a dictionary."""
        return {
            'repo_name': self.repo_name,
            'repo_url': self.repo_url,
            'file_results': [file_result.to_dict() for file_result in self.file_results]
        }

    def to_json(self) -> str:
        """Convert the RepoAnalysisResult instance to a JSON string."""
        import json
        return json.dumps(self.to_dict(), indent=4)

    def save(self):
        # save queries to a file
        if not os.path.exists(QUERIES_DIR):
            os.makedirs(QUERIES_DIR)
        url_without_protocol = self.repo_url.replace('https://', '').replace('http://', '')
        url_without_protocol = url_without_protocol.replace('github.com/', '')
        storage_dir_name = f"{url_without_protocol.replace('/', '_')}"
        storage_dir_name = os.path.join(QUERIES_DIR, storage_dir_name)

        # create a dir if it does not exist
        if not os.path.exists(storage_dir_name):
            os.makedirs(storage_dir_name, exist_ok=True)

        # save the file results file by file in the directory
        for file_result in self.file_results:
            if file_result.queries is None or len(file_result.queries) == 0:
                continue
            file_name = os.path.basename(file_result.file_path)
            file_name = file_name.split('.')[0] + '.json'  # Save as JSON file
            file_path = os.path.join(storage_dir_name, file_name)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(file_result.to_json())



def extract_sql_queries(file_path: str, repo_url: str, params: Optional[SqlExtractionParams] = None) -> FileAnalysisResult:
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} does not exist.")
        return FileAnalysisResult(file_path, repo_url, [])

    if not any(file_path.endswith(ext) for ext in SOURCE_CODE_FILE_EXTENSIONS):
        logger.warning(f"File {file_path} is not a recognized source code file.")
        return FileAnalysisResult(file_path, repo_url,[])

    # Initialize parameters if not provided
    if params is None:
        params = SqlExtractionParams()

    # Read the file content
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            content = file.read()
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return FileAnalysisResult(file_path, repo_url, [])

    # Extract SQL functions based on file extension
    sql_queries = []

    # if file_path.endswith('.sql'):
    #     # For SQL files, extract entire SQL statements
    #     sql_queries = extract_sql_from_sql_file(content, params)
    # else:
    # For other source code files, extract SQL strings
    sql_queries = extract_sql_from_source_code(content, file_path, params)

    return FileAnalysisResult(repo_url, file_path, sql_queries)


def extract_sql_from_source_code(content: str, file_path: str, params: SqlExtractionParams) -> List[SqlQuery]:
    """Extract SQL statements from any source code file using a unified pattern."""
    sql_functions = []

    # if the file is a sql file, we can directly extract the SQL statements, split them by semicolon
    if file_path.endswith('.sql'):
        strings = content.split(';')
    else:
        # Unified pattern: capture anything between quotes that might contain SQL
        strings = extract_strings(file_path)

    logger.debug(f"Extracted {len(strings)} potential SQL Queries from file {file_path}.")

    for string in strings:

        if not string or not looks_like_sql(string):
            continue

        cleaned_string = tidy_up_string(string)

        if should_include_sql(cleaned_string, params):
            query_type = determine_query_type(cleaned_string)
            sql_functions.append(
                SqlQuery(
                    name=f"{query_type}_query_{len(sql_functions) + 1}",
                    type=query_type,
                    sql=cleaned_string,
                    line=get_line_number(content, string)
                )
            )

    logger.debug(f"Extracted {len(sql_functions)} SQL queries from file {file_path}.")
    return sql_functions


# Need to Associate atleast one valueset to the code:
def looks_like_sql(text: str) -> bool:
    main_start_words = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP']
    # also with one whitespace behind
    main_start_words = [word + ' ' for word in main_start_words]
    # only allow the first word to be a main SQL command
    if not text.strip() or not any(text.strip().upper().startswith(word) for word in main_start_words):
        return False

    enable_only_allow_select = True
    if enable_only_allow_select and not text.strip().upper().startswith('SELECT'):
        return False

    sql_keywords = {
        'SELECT', 'FROM', 'WHERE', 'ALTER',
        'JOIN', 'LEFT', 'RIGHT',
        'INNER', 'OUTER', 'CREATE', 'TABLE', 'DROP',
        'UNION', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET',
    }
    # after these keywords, there is usually a space or a parenthesis
    sql_keywords = {kw + ' ' for kw in sql_keywords} | {kw + '(' for kw in sql_keywords}
    text_upper = text.upper()

    # count how many SQL keywords are present in the text
    keyword_count = sum(1 for kw in sql_keywords if kw in text_upper)
    # if there are at least 2 SQL keywords, it looks like SQL
    return keyword_count >= 2


_FIRST_KEYWORD_RE = re.compile(
    r"""
    ^\s*                                         # leading whitespace
    (?:
        (?:--[^\n]*\n\s*)*                       #   →  any “-- …” single-line comments
      | (?:/\*.*?\*/\s*)*                        #   →  any “/* … */” block comments
    )*                                           # zero or more comment blocks
    (?P<kw>[A-Z]+)                               # capture the first keyword
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)

# Map the first keyword (upper-case) to a logical query type
_KEYWORD_MAP: Dict[str, str] = {
    "SELECT":    "select",
    "INSERT":    "insert",
    "UPDATE":    "update",
    "DELETE":    "delete",
    "ALTER":     "alter",
    "DROP":      "drop",
    "TRUNCATE":  "truncate",
    "GRANT":     "grant",
    "REVOKE":    "revoke",
    "EXPLAIN":   "explain",
    "BEGIN":     "transaction",
    "START":     "transaction",
    "COMMIT":    "transaction",
    "ROLLBACK":  "transaction",
    "CALL":      "call",
    "MERGE":     "merge",
    "WITH":      "select",      # CTEs always resolve to a SELECT
}

def determine_query_type(sql_string: str) -> str:
    """
    Guess the *primary* type of an SQL statement by inspecting its first keyword.

    - Leading whitespace and both `--` and `/* … */` comments are skipped.
    - Common statements are mapped explicitly via `_KEYWORD_MAP`.
    - `CREATE …` is further split into `function`, `procedure`, or generic `create`.

    Returns
    -------
    One of: 'select', 'insert', 'update', 'delete', 'function', 'procedure',
    'create', 'alter', 'drop', … or 'unknown' if no match.
    """
    m = _FIRST_KEYWORD_RE.match(sql_string)
    if not m:
        return "unknown"

    kw = m.group("kw").upper()

    # Fast path: direct mapping
    if kw in _KEYWORD_MAP:
        return _KEYWORD_MAP[kw]

    if kw == "CREATE":
        up = sql_string.upper()
        if " FUNCTION " in up or up.startswith("CREATE FUNCTION"):
            return "function"
        if " PROCEDURE " in up or up.startswith("CREATE PROCEDURE"):
            return "procedure"
        return "create"

    return "unknown"


def should_include_sql(sql_string: str, params: SqlExtractionParams) -> bool:
    """Check if a SQL string should be included based on the parameters."""
    # If no tables or columns are specified, include all SQL
    if not params.tables and not params.columns:
        return True

    # Convert to uppercase for case-insensitive comparison
    sql_upper = sql_string.upper()

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


def extract_sql_from_repo(repo_dir: str, repo_url: str, params: Optional[SqlExtractionParams] = None) -> List[FileAnalysisResult]:
    results = []

    code_files = get_repo_files(repo_dir)
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
