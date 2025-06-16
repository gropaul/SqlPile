import logging
import os
import sys
from typing import Literal

# Data directories

# traverse one up as this is in src/config.py
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT, "data")
REPO_DIR = os.path.join(DATA_DIR, "repos")
LOG_DIR = os.path.join(DATA_DIR, "logs")
QUERIES_DIR = os.path.join(DATA_DIR, "queriesv2")
COMBINED_QUERIES_PATH = os.path.join(DATA_DIR, "combined_queries.parquet")
DATABASE_PATH = os.path.join(DATA_DIR, 'schemapilev2.duckdb')

# config
ONLY_SCRAPE_SELECT_QUERIES = False
CHARACTERS_BEFORE_AND_AFTER_QUERY = 150

type RepoHandling = Literal['delete_after_processing', 'compress_after_processing', 'keep_after_processing']
# How to handle repositories after processing
REPO_HANDLING: RepoHandling = 'delete_after_processing'  # Options: 'delete_after_processing', 'compress_after_processing', 'keep_after_processing'

PROCESS_ZIPPED_REPOS = False
LOG_TO_FILE = False  # Whether to log to a file or not

# create all directories if they do not exist
DIRS = [DATA_DIR, REPO_DIR, LOG_DIR, QUERIES_DIR]

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

    if LOG_FILE:
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
