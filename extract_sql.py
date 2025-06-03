import os
from dataclasses import dataclass
from typing import List, Optional, Dict

from config import logger, SOURCE_CODE_FILE_EXTENSIONS, QUERIES_DIR
from extract_strings import extract_strings


@dataclass
class SqlExtractionParams:
    """Helper data class for SQL extraction parameters."""
    tables: Optional[List[str]] = None
    columns: Optional[List[str]] = None


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
    def __init__(self, repo_name: str, queries: List[SqlQuery]):
        self.repo_name = repo_name
        self.queries = queries

    def to_dict(self) -> Dict[str, List[Dict[str, str]]]:
        """Convert the RepoAnalysisResult instance to a dictionary."""
        return {
            'repo_name': self.repo_name,
            'queries': [query.to_dict() for query in self.queries]
        }

    def to_json(self) -> str:
        """Convert the RepoAnalysisResult instance to a JSON string."""
        import json
        return json.dumps(self.to_dict(), indent=4)

    def save(self):
        # save queries to a file
        queries_file = os.path.join(QUERIES_DIR, f"{self.repo_name}_queries.json")

        with open(queries_file, 'w') as f:
            f.write('[\n')
            for (i, query) in enumerate(self.queries):
                if i > 0:
                    f.write(',\n')
                f.write(query.to_json())

            f.write(']')



def extract_sql_queries(file_path: str, params: Optional[SqlExtractionParams] = None) -> List[SqlQuery]:
    """
    Extract SQL functions from a source code file.

    Args:
        file_path: Path to the source code file
        params: Optional parameters for filtering SQL functions

    Returns:
        List of dictionaries containing SQL function details
    """
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} does not exist.")
        return []

    if not any(file_path.endswith(ext) for ext in SOURCE_CODE_FILE_EXTENSIONS):
        logger.warning(f"File {file_path} is not a recognized source code file.")
        return []

    # Initialize parameters if not provided
    if params is None:
        params = SqlExtractionParams()

    # Read the file content
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            content = file.read()
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return []

    # Extract SQL functions based on file extension
    sql_queries = []

    # if file_path.endswith('.sql'):
    #     # For SQL files, extract entire SQL statements
    #     sql_queries = extract_sql_from_sql_file(content, params)
    # else:
        # For other source code files, extract SQL strings
    sql_queries = extract_sql_from_source_code(content, file_path, params)

    return sql_queries

from typing import List




def extract_sql_from_source_code(content: str, file_path: str, params: SqlExtractionParams) -> List[SqlQuery]:
    """Extract SQL statements from any source code file using a unified pattern."""
    sql_functions = []

    # Unified pattern: capture anything between quotes that might contain SQL
    strings = extract_strings(file_path)

    for cleaned_sql in strings:

        if not cleaned_sql or not looks_like_sql(cleaned_sql):
            continue

        if should_include_sql(cleaned_sql, params):
            query_type = determine_query_type(cleaned_sql)
            sql_functions.append(
                SqlQuery(
                    name=f"{query_type}_query_{len(sql_functions) + 1}",
                    type=query_type,
                    sql=cleaned_sql,
                    line=get_line_number(content, cleaned_sql)
                )
            )

    return sql_functions

# Need to Associate atleast one valueset to the code:
def looks_like_sql(text: str) -> bool:
    sql_keywords = {
        'SELECT', 'FROM', 'WHERE',
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



def determine_query_type(sql_string: str) -> str:
    """Determine the type of SQL query."""
    sql_upper = sql_string.upper()

    if 'SELECT' in sql_upper:
        return 'select'
    elif 'INSERT' in sql_upper:
        return 'insert'
    elif 'UPDATE' in sql_upper:
        return 'update'
    elif 'DELETE' in sql_upper:
        return 'delete'
    elif 'CREATE' in sql_upper and 'FUNCTION' in sql_upper:
        return 'function'
    elif 'CREATE' in sql_upper and 'PROCEDURE' in sql_upper:
        return 'procedure'
    elif 'CREATE' in sql_upper:
        return 'create'
    elif 'ALTER' in sql_upper:
        return 'alter'
    elif 'DROP' in sql_upper:
        return 'drop'
    else:
        return 'unknown'


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


def extract_sql_from_repo(repo_dir: str, params: Optional[SqlExtractionParams] = None) -> List[SqlQuery]:
    queries = []

    code_files = get_repo_files(repo_dir)
    for file_path in code_files:
        print(f"Processing {file_path} for SQL extraction...")
        new_queries = extract_sql_queries(file_path, params)
        queries.extend(new_queries)
    print(f"Total SQL functions extracted: {len(queries)}")

    return queries





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



