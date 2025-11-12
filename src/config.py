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
LATEX_GEN_DIR = os.path.join(ROOT, "docs", "tex", "content", "gen")
LATEX_ASSETS_DIR = os.path.join(ROOT, "docs", "tex", "assets")

KAGGLE_DATA_DIR = os.path.join(DATA_DIR, "kaggle")
KAGGLE_DATA_DB_PATH = os.path.join(KAGGLE_DATA_DIR, "kaggle_data.duckdb")
KAGGLE_DATASETS_DB_PATH = os.path.join(KAGGLE_DATA_DIR, "kaggle_datasets.duckdb")

QUERY_RUN_TIMEOUT_SECONDS = 3  # Timeout for running queries in seconds

TPC_DATA_DIR = os.path.join(DATA_DIR, "tpc")
SQL_STORM_DATA_DIR = os.path.join(DATA_DIR, "sql_storm")
SQL_STORM_REPO_DIR = os.path.join(ROOT, "external", "SQLStorm")

# config
ONLY_SCRAPE_SELECT_QUERIES = False
CHARACTERS_BEFORE_AND_AFTER_QUERY = 400
HEADER_N_LINES = 30  # Number of header lines to keep for each file that contains SQL queries
MAX_VALUES_TO_SAVE_PER_COLUMN = 5000  # Maximum number of values to save per column in the database
MAX_VALUES_TO_ANALYZE_PER_COLUMN = 122_880

RepoHandling = Literal['delete_after_processing', 'compress_after_processing', 'keep_after_processing']
# How to handle repositories after processing
REPO_HANDLING: RepoHandling = 'delete_after_processing'  # Options: 'delete_after_processing', 'compress_after_processing', 'keep_after_processing'

PROCESS_ZIPPED_REPOS = False
LOG_TO_FILE = False  # Whether to log to a file or not

# create all directories if they do not exist
DIRS = [DATA_DIR, PLOTS_DIR, REPO_DIR, LOG_DIR, QUERIES_DIR_RAW, TMP_DIR, DATABASE_TMP_DIR, LATEX_GEN_DIR,
        LATEX_ASSETS_DIR, TPC_DATA_DIR, SQL_STORM_DATA_DIR, KAGGLE_DATA_DIR]

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
CREATE OR REPLACE TEMP MACRO prepare_select_statically(sql) AS
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
    .regexp_replace('''(\\d{4})\\s*-\\s*(\\d{2})\\s*-\\s*(\\d{2})''', '''\\1-\\2-\\3''', 'g')

    -- transform quoted timestamp: 'YYYY - MM - DD HH:MM:SS' → 'YYYY-MM-DD HH:MM:SS'
    .regexp_replace('''(\\d{4})\\s*-\\ss*(\\d{2})\\s*-\\s*(\\d{2})\\s+(\\d{2}:\\d{2}:\\d{2})''', '''\\1-\\2-\\3 \\4''', 'g')

    -- remove JOIN FETCH clauses: from owner left join fetch owner.pets -> from owner left join pets
    .regexp_replace('(?i)(left|right|inner|outer)?\\s*join\\s+fetch\\s+\\w+\\.(\\w+)', '\\1 join \\2', 'g')

    -- remove INSERT IGNORE and REPLACE it with INSERT
    .replace('insert ignore', 'insert')

    -- date stuff 
      .regexp_replace('(?i)(date_format)(\s*\()', 'strftime\2', 'g')
      .regexp_replace('(?i)(to_timestamp)(\s*\()', 'strptime\2', 'g')
      .regexp_replace('(?i)(to_date)(\s*\()', 'strptime\2', 'g')
  
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


def get_con(path: str = DATABASE_PATH, read_only: bool = False, max_threads: Optional[int] = 16) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path, read_only=read_only)

    if max_threads is not None:
        core_count = os.cpu_count() or 1
        threads_to_use = min(max_threads, core_count)
        con.execute(f"PRAGMA threads={threads_to_use};")

    def format_number_as_percentage(value: float) -> str:
        """Format a number as a percentage with two decimal places."""
        if value is None:
            return "NULL"
        return f"{value * 100:.2f}%"

    # Register the custom function to format numbers as percentages
    con.create_function("as_percentage", format_number_as_percentage, [float], str, type="native")

    def unify_llm_type(semantic_type: Optional[str]) -> str:
        return semantic_type if semantic_type else 'Other'

    cum_sum_macro = """
        -- cumulative sum over a LIST (NULLs treated as 0)
        CREATE OR REPLACE TEMP MACRO list_cum_sum(xs) AS (
          list_transform(xs, lambda x, i :
            list_reduce(list_slice(xs, 1, i), lambda acc, y : acc + coalesce(y, 0))
          )
        );
    """
    con.execute(cum_sum_macro)

    con.create_function('unify_llm_type', unify_llm_type, null_handling='SPECIAL')

    try:
        con.execute("""
                          CREATE TEMP VIEW column_usages_unnested AS
                          (
                          SELECT *, unnest(column_ids) AS column_id
                          FROM column_usages
                          )
                          """)

        con.execute("""
                        CREATE OR REPLACE TEMP VIEW values_often AS
                        WITH filtered_values AS (
                            SELECT column_id, value
                            FROM column_values
                            WHERE value != 'example text' 
                                AND value != 'None'
                                AND length(value) > 0
                                AND value IS NOT NULL
                        ),
                        often AS (
                            SELECT column_id
                            FROM filtered_values
                            GROUP BY column_id
                            HAVING COUNT(value) > 0
                        )
                        SELECT column_id, value
                        FROM filtered_values
                        WHERE column_id IN (SELECT column_id FROM often);
                """)
    except Exception as e:
        logging.warning(
            f"Could not create views column_usages_unnested and values_often. The tables might not exist yet.")
        pass

    def unifiy_usage_types(usage_type: str) -> str:
        usage_to_operator_map = {
            'TOP_N_KEY': 'Order Key',
            'SCAN_LOOKUP': 'Scan',
            'SCAN_FILTER': 'Filter',  # When changing, make sure, Filtered Scan > Scan
            'PROJECTION': 'Projection',
            'ORDER_KEY': 'Order Key',
            'JOIN_KEY': 'Join Key',
            'GROUP_KEY': 'Group Key',
            'DISTINCT_KEY': 'Group Key',
            'FILTER': 'Filter',
            'AGGREGATE': 'Aggregate',  # When changing, make sure, Ungrouped Aggregate > Grouped Aggregate
            'WINDOW_EXPRESSION': 'Window Function',
            'JOIN_MATERIALIZATION': 'Payload Column',
            'ORDER_MATERIALIZATION': 'Payload Column',
        }

        if usage_type not in usage_to_operator_map:
            raise ValueError(
                f"Unknown usage type: '{usage_type}'. Known types: {list(usage_to_operator_map.keys())}"
            )
        return usage_to_operator_map[usage_type]

    con.create_function(
        "unifiy_usage_types",
        unifiy_usage_types,
        null_handling='SPECIAL',
    )

    def get_repo_origin(repo_url: str) -> str:

        if repo_url.startswith("https://github.com/3rd-party/3rd-party-tpc"):
            return "TPC"
        elif repo_url.startswith("https://github.com/3rd-party/3rd-party-sql-storm"):
            return "SQLStorm"
        elif repo_url.startswith("https://github.com/3rd-party/3rd-party-kaggle"):
            return "Kaggle"
        else:
            return "SqlPile"

    con.create_function("get_repo_origin", get_repo_origin, [str], str, type="native")

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

    def usage_type_to_operator(usage_type: str) -> str:
        usage_to_operator_map = {
            'TOP_N_KEY': 'Order By',
            'SCAN_LOOKUP': 'Scan',
            'SCAN_FILTER': 'Filtered Scan',  # When changing, make sure, Filtered Scan > Scan
            'PROJECTION': 'Projection',
            'ORDER_KEY': 'Order By',
            'JOIN_KEY': 'Join',
            'GROUP_KEY': 'Grouped Aggregate',
            'DISTINCT_KEY': 'Grouped Aggregate',
            'FILTER': 'Filter',
            'AGGREGATE': 'Ungrouped Aggregate',  # When changing, make sure, Ungrouped Aggregate > Grouped Aggregate
            'WINDOW_EXPRESSION': 'Window Function',
            'JOIN_MATERIALIZATION': 'Join',
            'ORDER_MATERIALIZATION': 'Order By',
        }

        if usage_type not in usage_to_operator_map:
            raise ValueError(
                f"Unknown usage type: '{usage_type}'. Known types: {list(usage_to_operator_map.keys())}"
            )

        return usage_to_operator_map[usage_type]

    # register the UDF
    con.create_function("get_table_name_udf", udf_get_table_name_from_create, null_handling="special")
    con.create_function("is_create_view_udf", udf_is_create_view, null_handling="special")
    con.create_function("usage_type_to_operator", usage_type_to_operator, [str], str, type="native")

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
REPO_TABLE_NAME = 'repos'
REPO_META_DATA_FILES_TABLE_NAME = 'repos_meta_data'

FILES_TABLE_NAME = 'files'
FILES_META_DATA_TABLE_NAME = 'repo_meta_data_files'

TABLES_TABLE_NAME = 'tables'
TABLES_DATA_FILES_TABLE_NAME = 'table_data_files'
TABLE_VALUES_COUNT_TABLE_NAME = 'table_values_count'

COLUMNS_TABLE_NAME = 'columns'
COLUMN_VALUES_TABLE_NAME = 'column_values'
COLUMN_USAGES_TABLE_NAME = 'column_usages'
COLUMN_USAGES_HISTORY_TABLE_NAME = 'column_usage_history'

QUERY_OPERATOR_TABLE_NAME = 'query_operators'
QUERY_OPERATOR_COMPONENTS_TABLE_NAME = 'query_operator_components'
QUERY_OPERATOR_COMPONENT_EXPRESSIONS = 'query_component_expressions'

QUERIES_TABLE_NAME = 'queries'
QUERIES_EXECUTABLE_TABLE_NAME = 'queries_executable'
QUERIES_ERROR_SELECT_TABLE_NAME = 'queries_error_select'
QUERIES_ERROR_CREATE_TABLE_NAME = 'queries_error_create'
QUERIES_ERROR_CREATE_VIEW_TABLE_NAME = 'queries_error_create_view'
QUERIES_ERROR_INSERT_TABLE_NAME = 'queries_error_insert'
