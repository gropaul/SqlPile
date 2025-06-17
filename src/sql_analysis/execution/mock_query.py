from itertools import product
from typing import Literal, List, Optional

import duckdb

from src.config import logger

MockType = Literal['int', 'float', 'str']

# Define mock values for each type
mock_values = {
    'int': '42',
    'float': '3.14',
    'str': "'example'",
}
# Returns all mock queries with parameters inserted
def mock_parameters(sql: str) -> List[str]:
    count = sql.count('?')

    if count == 0:
        return [sql]

    # Generate all possible combinations of mock values
    options_per_param = [list(mock_values.values())] * count
    combinations = product(*options_per_param)

    # Interpolate each combination into the SQL string
    queries = []
    for combo in combinations:
        query = sql
        for value in combo:
            query = query.replace('?', value, 1)
        queries.append(query)

    return queries

class MockQueryResult:
    def __init__(self, query: Optional[str], error: Optional[Exception] = None):
        self.query = query
        self.error = error

    def __repr__(self):
        return f"MockQueryResult(query={self.query}, error={self.error})"

    def was_successful(self) -> bool:
        return self.query is not None

# will return you the first query that does not raise an error or none if all queries raise an error
def try_to_mock_and_execute_query(sql: str, con: duckdb.DuckDBPyConnection) -> MockQueryResult:
    sql_mocks = mock_parameters(sql)
    successful_query = None
    last_error = None
    logger.info(f"Trying to mock and execute query: {sql}, total mocks: {len(sql_mocks)}")
    for mock in sql_mocks:
        try:
            con.execute(mock)
            successful_query = mock
            break

        except Exception as e:
            last_error = e
            pass

    return MockQueryResult(query=successful_query, error=last_error)
