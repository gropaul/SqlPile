from itertools import product
from typing import Literal, List, Optional, Dict
import json
import duckdb
from sqloxide import parse_sql, mutate_expressions

from src.config import logger
from src.sql_analysis.execution.models import Table, Column
from src.sql_analysis.execution.prepare_sql_for_execution import prepare_sql_statically

MockType = Literal['int', 'float', 'str']

# Define mock values for each type
mock_values = {
    'int': '42',
    'float': '3.14',
    'str': "'example'",
}


class MockParameter:
    def __init__(self, start: int, end: int, type_order: List[MockType]):
        self.start = start
        self.end = end
        self.type_order = type_order
        self.current_index = 0


# Returns all mock queries with parameters inserted
class MockQueryResult:
    def __init__(self, original_query: Optional[str], executed_query: Optional[str] = None,
                 error: Optional[Exception] = None,
                 logical_plan: Optional[Dict] = None,
                 logical_plan_optimized: Optional[Dict] = None,
                 logical_plan_optimized_detailed: Optional[Dict] = None,
                 physical_plan: Optional[Dict] = None,
                 successful: bool = True
                 ):
        self.original_query = original_query
        self.executable_sql = executed_query
        self.error = error
        self.logical_plan = logical_plan
        self.logical_plan_optimized = logical_plan_optimized
        self.logical_plan_optimized_detailed = logical_plan_optimized_detailed
        self.physical_plan = physical_plan
        self.successful = successful

    def was_successful(self) -> bool:
        return self.successful


def visit_placeholders_turn_null(expr):
    # Detect and replace placeholders
    if "Value" in expr:
        val = expr["Value"]
        if isinstance(val, dict):
            if "value" in val:
                val = val.get("value")
            if "Placeholder" in val:
                val["Placeholder"] = "1"

    return expr

def try_to_mock_and_execute_query( sandbox_con: duckdb.DuckDBPyConnection, sql: str, tables: List[Table]) -> MockQueryResult:
    original_successful_query = None
    executed_query = None
    last_error = None

    logical_plan_json = None
    logical_plan_optimized = None
    logical_plan_optimized_detailed = None
    physical_plan = None

    successful = True
    # we need to disable filter_pushdown, filter_pushdown as long as we mock values with null as this leads to
    # empty results opimization in duckdb
    setting_queries = """
        PRAGMA explain_output = 'all';
        SET disabled_optimizers = '';
        SET disabled_optimizers = 'compressed_materialization,statistics_propagation,filter_pushdown';
    """
    try:
        original_successful_query = sql
        rewritten_sql = prepare_sql_statically(sql)

        ast = parse_sql(sql=rewritten_sql, dialect='generic')
        nulled_sql = mutate_expressions(parsed_query=ast, func=visit_placeholders_turn_null)[0]
        executed_query = nulled_sql

        sql = f"{setting_queries}; EXPLAIN (FORMAT JSON) {executed_query};"
        plans = sandbox_con.execute(sql).fetchall()

        # dict_keys(['logical_plan', 'logical_opt', 'logical_opt_detailed', 'physical_plan'])
        logical_plan_json = json.loads(plans[0][1])[0]
        logical_plan_optimized = json.loads(plans[1][1])[0]
        logical_plan_optimized_detailed = json.loads(plans[2][1])[0]
        physical_plan = json.loads(plans[3][1])[0]

    except Exception as e:
        successful = False
        last_error = e

    return MockQueryResult(original_query=original_successful_query, executed_query=executed_query,
                           error=last_error,
                           logical_plan=logical_plan_json,
                           logical_plan_optimized=logical_plan_optimized,
                           logical_plan_optimized_detailed=logical_plan_optimized_detailed,
                           physical_plan=physical_plan, successful=successful)