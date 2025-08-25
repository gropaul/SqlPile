import json
from typing import List, Dict

import duckdb
import pandas as pd
from tqdm import tqdm

from src.config import DATABASE_PATH, logger, COLUMN_USAGES_TABLE_NAME, COLUMN_USAGES_HISTORY_TABLE_NAME
from src.sql_analysis.execution.models import Table, Column
from src.sql_analysis.execution.prepare_sql_for_execution import escape_for_insert
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


def analyse_plans(con: duckdb.DuckDBPyConnection , repo_id: int, query_id: int = None):

    current_usage_id = con.execute(f"""
        SELECT MIN(id) + 1 FROM {COLUMN_USAGES_TABLE_NAME}
    """).fetchone()[0]

    if current_usage_id is None:
        current_usage_id = 0

    plans_query = f"""
        SELECT 
            r.id AS repo_id,
            -- Subquery for queries
            (
                SELECT list({{
                    query_id: q.query_id,
                    sql: q.executable_sql,
                    plan: q.logical_plan_optimized_detailed
                }})
                FROM queries_executable q
                WHERE q.repo_id = r.id
                {" AND q.query_id = " + str(query_id) if query_id is not None else "" }
            ) AS queries,
            -- Subquery for tables and their columns
            (
                SELECT list({{
                    table_id: t.id,
                    table_name: t.table_name_clean,
                    columns: (
                        SELECT list({{
                            column_id: c.id,
                            column_name: c.column_name,
                            column_base_type: c.column_base_type
                        }})
                        FROM columns c
                        WHERE c.table_id = t.id
                    )
                }})
                FROM tables t
                WHERE t.repo_id = r.id
            ) AS tables
        FROM repos r 
        WHERE EXISTS (
            FROM queries_executable q
            WHERE q.repo_id = r.id 
        ) AND repo_id = {repo_id}
    """
    plans = con.execute(plans_query).fetchall()

    for (repo_id, queries, tables) in tqdm(plans, desc="Analyzing plans", unit="repo"):
        tables_parsed = initialize_tables(tables)

        if not queries:
            logger.warning(f"No queries found for repo ID: {repo_id}")
            continue
        n_queries = len(queries)



        for query_index, query in enumerate(queries):
            query_id = query['query_id']
            plan = json.loads(query['plan'])
            logger.info(f"Analyzing query ID: {query_id} ({query_index + 1}/{n_queries}) in repo ID: {repo_id}: {query['sql']}")
            column_usages, tracks = analyze_node(query_id, plan, tables_parsed, [], )

            history_rows = []
            column_usages_row = []

            # Insert results into the COLUMN_USAGES_TABLE_NAME
            for column_usage in column_usages:
                column_usages_row.append({
                    "id": current_usage_id,
                    "query_id": query_id,
                    "node_id": column_usage.node_id,
                    "column_ids": json.dumps(column_usage.column_ids),
                    "expression": escape_for_insert(column_usage.expression),
                    "expression_result_type": column_usage.expression_result_type,
                    "usage_type": column_usage.usage_type,
                    "meta_data": json.dumps(column_usage.meta_data)
                })
                for history in column_usage.column_id_histories:
                    history_rows.append({
                        "usage_id": current_usage_id,
                        "column_id": history.column_id,
                        "history": history.history_to_dict()
                    })

                current_usage_id += 1


            column_usages_df = pd.DataFrame(column_usages_row, columns=["id", "query_id", "node_id", "column_ids", "expression", "expression_result_type", "usage_type", "meta_data"])
            insert_query = f"""
                                INSERT INTO {COLUMN_USAGES_TABLE_NAME} ( id, query_id, node_id, column_ids, expression, expression_result_type, usage_type, meta_data)
                                SELECT * FROM column_usages_df
                            """
            con.execute(insert_query)

            history_df = pd.DataFrame(history_rows, columns=["usage_id", "column_id", "history"])
            insert_history_query = f"""
                               INSERT INTO {COLUMN_USAGES_HISTORY_TABLE_NAME} (usage_id, column_id, history)
                               SELECT * FROM history_df
                           """
            con.execute(insert_history_query)





if __name__ == "__main__":
    con = duckdb.connect(DATABASE_PATH)
    query_id = 12177818
    repo_id = 23184
    analyse_plans(con, repo_id, query_id)
