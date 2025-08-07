import logging
import os
import sys
import re
import duckdb
from typing import Literal, Optional

# Data directories

# traverse one up as this is in src/config.py
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT, "data")
SCHEMAPILE_DIR = os.path.join(DATA_DIR, "schemapile")
PLOTS_DIR = os.path.join(ROOT, "plots")
REPO_DIR = os.path.join(DATA_DIR, "repos")
LOG_DIR = os.path.join(DATA_DIR, "logs")
QUERIES_DIR_RAW = os.path.join(DATA_DIR, "queries_raw")
QUERIES_DIR_FROM_CLUSTER = os.path.join(DATA_DIR, "queries_from_cluster")
QUERIES_DIR_PARTITIONED = os.path.join(DATA_DIR, "queries_partitioned")
DATABASE_PATH = os.path.join(DATA_DIR, 'schemapile.duckdb')
TMP_DIR = os.path.join(DATA_DIR, "tmp")
DATABASE_TMP_DIR = os.path.join(TMP_DIR, "databases")
GENERATED_SECTIONS_DIR = os.path.join(ROOT, "docs", "tex", "gen")

# config
ONLY_SCRAPE_SELECT_QUERIES = False
CHARACTERS_BEFORE_AND_AFTER_QUERY = 400
HEADER_N_LINES = 30  # Number of header lines to keep for each file that contains SQL queries

RepoHandling = Literal['delete_after_processing', 'compress_after_processing', 'keep_after_processing']
# How to handle repositories after processing
REPO_HANDLING: RepoHandling = 'delete_after_processing'  # Options: 'delete_after_processing', 'compress_after_processing', 'keep_after_processing'

PROCESS_ZIPPED_REPOS = False
LOG_TO_FILE = False  # Whether to log to a file or not

# create all directories if they do not exist
DIRS = [DATA_DIR, PLOTS_DIR, REPO_DIR, LOG_DIR, QUERIES_DIR_RAW, TMP_DIR, DATABASE_TMP_DIR, GENERATED_SECTIONS_DIR]

for directory in DIRS:
    if not os.path.exists(directory):
        os.makedirs(directory)

# Logging configuration
# Set the default logging level - can be changed to DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL = logging.INFO
LOG_FILE = os.path.join(LOG_DIR, "sqlpile.log")

SOURCE_CODE_FILE_EXTENSIONS = [
    ".py",  # Python
    ".java",  # Java
    ".js",  # JavaScript
    ".ts",  # TypeScript
    ".rb",  # Ruby
    ".php",  # PHP
    ".go",  # Go
    ".cs",  # C#
    ".sql",  # Raw SQL files
    ".scala",  # Scala
    ".kt",  # Kotlin
    ".swift",  # Swift
    ".pl",  # Perl
    ".dart",  # Dart
    ".r",  # R
    ".erl",  # Erlang
    ".ex",  # Elixir
    ".exs",  # Elixir script
    ".cpp",  # C++ (e.g. embedded queries)
    ".c",  # C
    ".h",  # C header files (e.g. embedded queries)
    ".hpp",  # C++ header files (e.g. embedded queries)
    ".html",  # e.g. inline SQL in templates
    ".xml",  # e.g. MyBatis, Android Room queries
    ".jsp",  # Java Server Pages
    ".vue",  # Vue components with embedded SQL
    ".tsx",  # React components with inline queries
    ".jsx"  # React components with inline queries
]

PREPARE_SQL_STATICALLY_MACRO = """
CREATE OR REPLACE MACRO prepare_select_statically(sql) AS
    sql
    -- backticks → double quotes
    .replace('`', '"')
    -- fix spaced comparisons
    .replace('> =', '>=')
    .replace('< =', '<=')
    .replace('! =', '!=')
    .replace('= =', '=')
    -- transform ${x} → #{x}
    .regexp_replace('\\$\\{(\\w+)\\}', '#{\\1}', 'g')
    -- transform quoted date: 'YYYY - MM - DD' → 'YYYY-MM-DD'
    .regexp_replace('''(\d{4})\s*-\s*(\d{2})\s*-\s*(\d{2})''', '''\\1-\\2-\\3''', 'g')

    -- transform quoted timestamp: 'YYYY - MM - DD HH:MM:SS' → 'YYYY-MM-DD HH:MM:SS'
    .regexp_replace('''(\d{4})\s*-\s*(\d{2})\s*-\s*(\d{2})\s+(\d{2}:\d{2}:\d{2})''', '''\\1-\\2-\\3 \\4''', 'g')

    -- remove JOIN FETCH clauses: from owner left join fetch owner.pets -> from owner left join pets
    .regexp_replace('(?i)(left|right|inner|outer)?\s*join\s+fetch\s+\w+\.(\w+)', '\\1 join \\2', 'g')

    -- remove INSERT IGNORE and REPLACE it with INSERT
    .replace('insert ignore', 'insert')
    -- function renames
    .replace('date_format', 'strftime')

    -- date stuff 
    .replace('to_timestamp', 'strptime')
    .replace('to_date', 'strptime')

    -- Replace common Java-style or MySQL-style format strings with DuckDB-style
    .replace('yyyy - mm - dd', '%Y - %m - %d')
    .replace('yyyy-mm-dd', '%Y-%m-%d')
    .replace('yyyy/mm/dd', '%Y/%m/%d')
    .replace('hh24:mi:ss', '%H:%M:%S')  
    .replace('hh:mi:ss', '%I:%M:%S')    
    .replace('h:m:s', '%H:%M:%S')
    .replace('h:m', '%H:%M')
    .replace('hh24', '%H')
    -- RAND() → RANDOM()
    .regexp_replace('(?i)\\brand\\s*\\(\\s*\\)', 'random()', 'g')
    -- %s, %i, %d → :param_name
    .regexp_replace('%[0-9.+\-#]*[a-zA-Z]', ':param_name', 'g')
    -- #{param} → :param
    .regexp_replace('#\\{(\\w+)\\}', ':\\1', 'g')
    -- {param} (f‑string style) → :param
    .regexp_replace('\\{\\s*(\\w+)\\s*\\}', ':\\1', 'g')
    -- LIMIT a, b → LIMIT b OFFSET a
    .regexp_replace('(?i)limit\\s+(\\d+)\\s*,\\s*(\\d+)',
                    'limit \\2 offset \\1',
                    'g')
;
"""

def get_con(path: str = DATABASE_PATH, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path, read_only=read_only)

    def udf_get_table_name_from_create(query: str) -> Optional[str]:
        # Remove extra whitespace and normalize casing for matching
        cleaned_query = re.sub(r'\s+', ' ', query.strip()).lower()
        # rewrite "create temporary table" or "create temp table" to "create table"
        cleaned_query = re.sub(r'\bcreate\s+(temporary|temp)\s+table\b', 'create table', cleaned_query)
        # rewrite "create table if not exists" to "create table"
        cleaned_query = re.sub(r'\bcreate\s+table\s+if\s+not\s+exists\b', 'create table', cleaned_query)
        # rewrite "create or replace table" to "create table"
        cleaned_query = re.sub(r'\bcreate\s+or\s+replace\s+table\b', 'create table', cleaned_query)

        # regex pattern to match CREATE TABLE statements
        pattern = re.compile(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([`"\[\]\w\.]+)',
            re.IGNORECASE
        )

        match = pattern.match(cleaned_query)
        if match:
            name = match.group(1).strip('`"[]')
            # if the table name is a qualified name (e.g., schema.table), return only the table name
            if '.' in name:
                name = name.split('.')[-1]
            return name.lower()
        else:
            return None

    def udf_is_create_view(query: str) -> bool:
        """Check if the query is a CREATE VIEW, CREATE MATERIALIZED VIEW, or CREATE TEMP VIEW."""
        cleaned_query = re.sub(r'\s+', ' ', query.strip()).lower()

        pattern = re.compile(
            r'''
            ^create                                   # must start with 'create'
            (\s+or\s+replace)?                        # optional 'or replace'
            (\s+temp(?:orary)?)?                      # optional 'temp' or 'temporary'
            (\s+materialized)?                        # optional 'materialized'
            \s+view                                   # 'view' keyword
            (\s+if\s+not\s+exists)?                   # optional 'if not exists'
            \s+[`"\[\]\w\.]+                          # identifier follows
            ''',
            re.IGNORECASE | re.VERBOSE
        )

        return bool(pattern.match(cleaned_query))

    # register the UDF
    con.create_function("get_table_name_udf", udf_get_table_name_from_create, null_handling="special")
    con.create_function("is_create_view_udf", udf_is_create_view, null_handling="special")
    con.execute(PREPARE_SQL_STATICALLY_MACRO)

    return con

# Configure logging
def setup_logging():
    """Set up logging configuration."""
    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(LOG_LEVEL)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler.setFormatter(formatter)

    if LOG_FILE and False:
        # Create file handler
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setLevel(LOG_LEVEL)
        file_handler.setFormatter(formatter)
        # Add the handlers to the logger
        logger.addHandler(file_handler)

    logger.addHandler(console_handler)

    return logger


# Initialize logger
logger = setup_logging()
