from src.config import QUERIES_EXECUTABLE_TABLE_NAME, QUERIES_ERROR_SELECT_TABLE_NAME, REPO_TABLE_NAME, \
    QUERIES_ERROR_CREATE_TABLE_NAME, QUERIES_ERROR_CREATE_VIEW_TABLE_NAME, QUERIES_ERROR_INSERT_TABLE_NAME, \
    COLUMN_VALUES_TABLE_NAME, TABLE_VALUES_COUNT_TABLE_NAME, COLUMN_USAGES_TABLE_NAME, COLUMN_USAGES_HISTORY_TABLE_NAME, \
    QUERY_OPERATOR_TABLE_NAME, QUERY_OPERATOR_COMPONENTS_TABLE_NAME, QUERY_OPERATOR_COMPONENT_EXPRESSIONS, \
    QUERIES_TABLE_NAME, REPOS_PROCESSED_TABLE_NAME
from src.sql_analysis.execution.models import ExecutionMode
from src.sql_analysis.load_schemapile_json_to_ddb import primary_key, foreign_key
import duckdb


def create_base_tables(con: duckdb.DuckDBPyConnection, mode: ExecutionMode):
    if mode == 'restart':
        create_statement = "CREATE OR REPLACE TABLE"
    elif mode == 'append' or mode == 'replace':
        create_statement = "CREATE TABLE IF NOT EXISTS"
    else:
        raise ValueError(f"Unknown mode: {mode}")

    con.execute(f"""
        {create_statement} {QUERIES_EXECUTABLE_TABLE_NAME} (
            id BIGINT {primary_key()},
            query_id BIGINT,
            repo_id BIGINT,
            original_sql VARCHAR,
            executable_sql VARCHAR,
            logical_plan JSON,
            logical_plan_optimized JSON,
            logical_plan_optimized_detailed JSON,
            physical_plan JSON
        )
    """)

    # create processed table with the repo id to track which repos have been processed
    con.execute(f"""
        {create_statement} {REPOS_PROCESSED_TABLE_NAME} (
            repo_id BIGINT {foreign_key(REPO_TABLE_NAME, 'id')},
            repo_url VARCHAR,
            processed_at TIMESTAMP
        )
    """)

    # create error table if it doesn't exist
    con.execute(f"""
        {create_statement} {QUERIES_ERROR_SELECT_TABLE_NAME} (
            id BIGINT {primary_key()},
            repo_id BIGINT {foreign_key(REPO_TABLE_NAME, 'id')},
            repo_url VARCHAR,
            query_id BIGINT,
            error_message VARCHAR,
            original_sql VARCHAR,
            executable_sql VARCHAR,
        )
    """)

    # con.execute(f"""
    #     {create_statement} {QUERY_OPERATOR_TABLE_NAME} (
    #         id BIGINT {primary_key()},
    #         query_id BIGINT,
    #         node_id VARCHAR,
    #         node_type VARCHAR,
    #         meta_data JSON
    #     )
    # """)
    #
    # con.execute(f"""
    #     {create_statement} {QUERY_OPERATOR_COMPONENTS_TABLE_NAME} (
    #         id BIGINT {primary_key()},
    #         operator_id BIGINT {foreign_key(QUERY_OPERATOR_TABLE_NAME, 'id')},
    #         component_type VARCHAR
    #     )
    # """)
    #
    # con.execute(f"""
    #     {create_statement} {QUERY_OPERATOR_COMPONENT_EXPRESSIONS} (
    #         id BIGINT {primary_key()},
    #         component_id BIGINT {foreign_key(QUERY_OPERATOR_COMPONENTS_TABLE_NAME, 'id')},
    #         expression JSON,
    #     )
    # """)

    # create a view that contains all the ids of queries that have been already executed
    con.execute(f"""
        CREATE OR REPLACE VIEW executed_queries_ids AS
        (
            with ids AS (
                SELECT query_id FROM {QUERIES_EXECUTABLE_TABLE_NAME}
                UNION SELECT query_id FROM {QUERIES_ERROR_SELECT_TABLE_NAME}
            ) SELECT DISTINCT query_id FROM ids
        )
        """)

    con.execute(f"""
        {create_statement} {QUERIES_ERROR_CREATE_TABLE_NAME} (
            table_id BIGINT,
            table_name VARCHAR,
            error_message VARCHAR
        )
    """)

    con.execute(f"""
        {create_statement} {QUERIES_ERROR_CREATE_VIEW_TABLE_NAME} (
            query_id BIGINT,
            error_message VARCHAR
        )
    """)

    con.execute(f"""
        {create_statement} {QUERIES_ERROR_INSERT_TABLE_NAME} (
            query_id BIGINT,
            error_message VARCHAR,
            )
    """)

    # Create a table to store the column values with strings
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {COLUMN_VALUES_TABLE_NAME} (
            column_id BIGINT,
            value VARCHAR,
        )
    """)

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_VALUES_COUNT_TABLE_NAME} (
            table_id BIGINT,
            count INTEGER
        )
    """)

    create_usages_table_query = f"""
        {create_statement} {COLUMN_USAGES_TABLE_NAME} (
            id INTEGER,
            query_id INTEGER,
            node_id VARCHAR,
            column_ids INTEGER[],
            expression VARCHAR,
            expression_result_type VARCHAR,
            usage_type VARCHAR,
            meta_data JSON
        )
       """
    con.execute(create_usages_table_query)

    create_usage_history_table_query = f"""
        {create_statement} {COLUMN_USAGES_HISTORY_TABLE_NAME} (
            usage_id INTEGER,
            column_id INTEGER,
            history STRUCT(
                    expression VARCHAR,
                    expression_type VARCHAR,
                    expression_class VARCHAR,
                    expression_result_type VARCHAR
                )[]
            )
    """
    con.execute(create_usage_history_table_query)
