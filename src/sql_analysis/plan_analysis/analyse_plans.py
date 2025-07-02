import json
from typing import List, Dict, Literal

import duckdb

from src.config import DATABASE_PATH
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

    return tables



def analyse_plans():
    con = duckdb.connect(DATABASE_PATH)
    plans_query = """
        WITH 
            tables_dedup AS (
                SELECT MIN(t.id) as id, t.table_name, t.repo_id
                FROM tables t 
                GROUP BY t.table_name, t.repo_id
            )
        SELECT 
            t.repo_id,
            list({
                id: q.id,
                plan: q.logical_plan_optimized_detailed, 
                sql: q.executable_sql
            }) AS queries,
            list({
                table_id: t.id,
                table_name: t.table_name,
                columns: (
                    SELECT list({ 
                        column_id: c.id,
                        column_name: c.column_name,
                        column_base_type: c.column_base_type,
                    })
                    FROM columns c
                    WHERE c.table_id = t.id
                )
            }) AS tables
        FROM (FROM queries_executable) as q
        JOIN tables_dedup t ON q.repo_id = t.repo_id
        GROUP BY t.repo_id -- Group by table_name to avoid duplicates
    """
    plans = con.execute(plans_query).fetchall()

    for (repo_id, queries, tables) in plans:
        for query in queries:
            id = query['id']
            plan = json.loads(query['plan'])
            analyze_node(id, plan, initialize_tables(tables))

if __name__ == "__main__":
    analyse_plans()