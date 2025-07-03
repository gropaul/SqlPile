import json
from typing import List, Dict, Literal

import duckdb

from src.config import DATABASE_PATH, logger
from src.sql_analysis.execution.models import Table, Column
from src.sql_analysis.plan_analysis.analyze_nodes import analyze_node


def initialize_tables(table_structs: List[Dict]) -> List[Table]:

    tables = []
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

    for (repo_id, queries, tables) in plans:
        tables_parsed = initialize_tables(tables)

        for query in queries:
            id = query['id']
            plan = json.loads(query['plan'])
            logger.info(f"Analyzing query ID: {id}, Repo ID: {repo_id}, SQL: {query['sql']}")
            results, tracks = analyze_node(id, plan, tables_parsed)

            # print the plan nicely formatted
            # logger.info(f"Plan for query ID {id}: {json.dumps(plan, indent=2)}")

            for results in results:
                logger.info(f"Query ID: {id}, Operator: {results.usage_type}, Expression: {results.expression}, Columns: {results.column_ids}")

if __name__ == "__main__":
    analyse_plans()