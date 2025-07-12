import json
from typing import List, Dict, Literal
from tqdm import tqdm
import duckdb

from src.config import DATABASE_PATH, logger
from src.sql_analysis.execution.models import Table, Column
from src.sql_analysis.execution.prepare_sql_for_execution import escape_for_insert
from src.sql_analysis.load_schemapile_json_to_ddb import COLUMN_USAGES_TABLE_NAME
from src.sql_analysis.plan_analysis.analyze_nodes import analyze_node


def initialize_tables(table_structs: List[Dict]) -> List[Table]:

    tables = []

    if table_structs is None:
        logger.warning("No table structures provided, returning empty list.")
        return tables

    for table_struct in table_structs:
        if 'columns' not in table_struct:
            table_struct['columns'] = []
        elif table_struct['columns'] is None:
            table_struct['columns'] = []
        else:
            # check if there are any elements in the list, if yes, check if the first is already a Column object
            if len(table_struct['columns']) == 0:
                continue
            if isinstance(table_struct['columns'][0], Column):
                continue
            table_struct['columns'] = [Column(**column_struct) for column_struct in table_struct['columns']]

        table = Table(**table_struct)

        tables.append(table)

    # if the length of the table structs is larger then the length of the tables, it means some tables were not initialized
    if len(table_structs) > len(tables):
        logger.error(f"Some tables were not initialized: {len(table_structs) - len(tables)}")

    return tables


def analyse_plans(con: duckdb.DuckDBPyConnection , repo_id: int):

    min_usage_id = con.execute(f"""
        SELECT MIN(id) + 1 FROM {COLUMN_USAGES_TABLE_NAME}
    """).fetchone()[0]

    if min_usage_id is None:
        min_usage_id = 0

    plans_query = """
        SELECT 
            r.id AS repo_id,
            -- Subquery for queries
            (
                SELECT list({
                    query_id: q.query_id,
                    sql: q.executable_sql,
                    plan: q.logical_plan_optimized_detailed
                })
                FROM queries_executable q
                WHERE q.repo_id = r.id
            ) AS queries,
            -- Subquery for tables and their columns
            (
                SELECT list({
                    table_id: t.id,
                    table_name: t.table_name,
                    columns: (
                        SELECT list({
                            column_id: c.id,
                            column_name: c.column_name,
                            column_base_type: c.column_base_type
                        })
                        FROM columns c
                        WHERE c.table_id = t.id
                    )
                })
                FROM tables t
                WHERE t.repo_id = r.id
            ) AS tables
        FROM repos r 
        WHERE EXISTS (
            FROM queries_executable q
            WHERE q.repo_id = r.id 
        ) AND repo_id = 
    """ + str(repo_id)
    plans = con.execute(plans_query).fetchall()

    for (repo_id, queries, tables) in tqdm(plans, desc="Analyzing plans", unit="repo"):
        tables_parsed = initialize_tables(tables)

        if not queries:
            logger.warning(f"No queries found for repo ID: {repo_id}")
            continue

        for query in queries:
            query_id = query['query_id']
            plan = json.loads(query['plan'])
            logger.info(f"Analyzing query ID: {query_id}, Repo ID: {repo_id}, SQL: {query['sql']}")
            results, tracks = analyze_node(query_id, plan, tables_parsed, [])

            # Insert results into the COLUMN_USAGES_TABLE_NAME
            for result in results:
                # logger.info(f"Query ID: {id}, Operator: {result.usage_type}, Expression: {result.expression}, Columns: {result.column_ids}")
                insert_query = f"""
                    INSERT INTO {COLUMN_USAGES_TABLE_NAME} ( id, query_id, column_ids, expression, expression_result_type, usage_type)
                    VALUES ({min_usage_id}, {query_id}, {json.dumps(result.column_ids)}, '{escape_for_insert(result.expression)}', '{result.expression_result_type}', '{result.usage_type}')
                """
                con.execute(insert_query)
                min_usage_id += 1





if __name__ == "__main__":
    con = duckdb.connect(DATABASE_PATH)

    analyse_plans(con, 27994)
