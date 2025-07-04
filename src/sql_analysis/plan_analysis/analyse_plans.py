import json
from typing import List, Dict, Literal

import duckdb
from tqdm import tqdm

from src.config import DATABASE_PATH, logger
from src.sql_analysis.execute_queries import escape_string
from src.sql_analysis.execution.models import Table, Column
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



def analyse_plans():
    con = duckdb.connect(DATABASE_PATH)

    create_usages_table_query = f"""
        CREATE OR REPLACE TABLE {COLUMN_USAGES_TABLE_NAME} (
            id INTEGER,
            query_id INTEGER,
            column_ids INTEGER[],
            expression VARCHAR,
            expression_result_type VARCHAR,
            usage_type VARCHAR)
    """
    con.execute(create_usages_table_query)

    n_added = 0

    plans_query = """
        SELECT 
            r.id AS repo_id,
            -- Subquery for queries
            (
                SELECT list({
                    id: q.id,
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
        );
    """
    plans = con.execute(plans_query).fetchall()

    for (repo_id, queries, tables) in tqdm(plans, desc="Analyzing plans", unit="repo"):
        tables_parsed = initialize_tables(tables)

        for query in queries:
            id = query['id']
            plan = json.loads(query['plan'])
            logger.info(f"Analyzing query ID: {id}, Repo ID: {repo_id}, SQL: {query['sql']}")
            results, tracks = analyze_node(id, plan, tables_parsed)

            # Insert results into the COLUMN_USAGES_TABLE_NAME
            for result in results:
                # logger.info(f"Query ID: {id}, Operator: {result.usage_type}, Expression: {result.expression}, Columns: {result.column_ids}")
                insert_query = f"""
                    INSERT INTO {COLUMN_USAGES_TABLE_NAME} ( id, query_id, column_ids, expression, expression_result_type, usage_type)
                    VALUES ({n_added}, {id}, {json.dumps(result.column_ids)}, '{escape_string(result.expression)}', '{result.expression_result_type}', '{result.usage_type}')
                """
                con.execute(insert_query)
                n_added += 1





if __name__ == "__main__":
    analyse_plans()