from itertools import product
from typing import Literal, List, Optional
import json
import duckdb
from sqloxide import parse_sql, mutate_expressions

from src.config import logger
from src.sql_analysis.execution.models import Table, Column

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
    def __init__(self, query: Optional[str], error: Optional[Exception] = None):
        self.query = query
        self.error = error

    def __repr__(self):
        return f"MockQueryResult(query={self.query}, error={self.error})"

    def was_successful(self) -> bool:
        return self.query is not None




def visit_placeholders_turn_null(expr):
    # Detect and replace placeholders
    if "Value" in expr:
        val = expr["Value"]
        if isinstance(val, dict):
            if "value" in val:
                val = val.get("value")
            if "Placeholder" in val:
                val["Placeholder"] = "null"

    return expr


def try_to_mock_and_execute_query(sql: str, con: duckdb.DuckDBPyConnection, tables: List[Table]) -> MockQueryResult:

    successful_query = None
    last_error = None

    # we need to disable filter_pushdown, filter_pushdown as long as we mock values with null as this leads to
    # empty results opimization in duckdb
    explain_wrapper = """
        PRAGMA explain_output = 'all';
        SET disabled_optimizers = 'empty_result_pullup,statistics_propagation,filter_pushdown,filter_pushdown';
        EXPLAIN (FORMAT json) """
    try:
        ast = parse_sql(sql=sql, dialect='generic')
        nulled_sql = mutate_expressions(parsed_query=ast, func=visit_placeholders_turn_null)[0]
        explain_sql = explain_wrapper + nulled_sql
        result = con.execute(explain_sql).fetchall()
        logical_plan_str = result[1][1]
        logical_plan_json = json.loads(logical_plan_str)[0]
        # print query and logical plan
        logger.info(f"Executing query: {sql}")
        logger.info(f"Logical plan: \n{json.dumps(logical_plan_json, indent=2)}")


        successful_query = sql
    except Exception as e:
        last_error = e
        logger.error(f"Error executing query: {e}")

    return MockQueryResult(query=successful_query, error=last_error)


example_sql = "SELECT * FROM my_table WHERE name = :name"
example_columns = [
    Column(column_id=1, column_name="id", column_base_type="int"),
    Column(column_id=2, column_name="name", column_base_type="str")
]
example_tables = [Table(table_id=1, table_name="my_table", columns=example_columns)]

if __name__ == "__main__":
    # Example usage
    con = duckdb.connect()
    con.execute("CREATE TABLE my_table (id INTEGER, name VARCHAR)")
    result = try_to_mock_and_execute_query(example_sql, con, example_tables)
    print(result)
    con.close()