from typing import Optional

import duckdb
import re

def wrap_any_value(sql: str, column_name: str, count: int = 1) -> str:
    """
    Replace the first occurrence of <alias>.column_name  *or* column_name
    with ANY_VALUE(<alias>.column_name) in the SQL text.
    """
    # 1)  optional alias and dot:   (?:\b\w+\.)?
    # 2)  the exact column name:    \bcolumn_name\b
    pattern = rf'(?:\b\w+\.)?\b{re.escape(column_name)}\b'

    def replacer(m: re.Match) -> str:
        full_identifier = m.group(0)          # 'articles.historyid'  or  'historyid'
        return f'any_value({full_identifier})'

    return re.sub(pattern, replacer, sql, count=count, flags=re.IGNORECASE)

def fix_group_by(sql: str, error: str, sandbox_con: duckdb.DuckDBPyConnection, depth: int = 0) -> Optional[str]:
    # 1. Check if the error is related to GROUP BY
    if 'must appear in the GROUP BY clause or must be part of an aggregate function' not in error:
        return None

    if depth > 10:
        # If we have tried to fix the GROUP BY clause too many times, return the original SQL
        return None

    # 2. Try to fix the GROUP BY clause
    """
    Binder Error: column "historyid" must appear in the GROUP BY clause or must be part of an aggregate function.
    Either add it to the GROUP BY list, or use "ANY_VALUE(historyid)" if the exact value of "historyid" is not important.
    """
    # Search for "ANY_VALUE(X)" in the error message and if it exists, retrieve the column name X
    any_value_match = re.search(r'ANY_VALUE\((\w+)\)', error)

    if not any_value_match:
        # If "ANY_VALUE(X)" is not in the error message, we cannot fix it
        return None

    column_name = any_value_match.group(1)
    # Replace the first occurrence of "X" in the SQL with "ANY_VALUE(X)"
    sql = wrap_any_value(sql, column_name)

    # try to execute the modified SQL
    try:
        sandbox_con.execute(sql)
        return sql
    except Exception as e:
        return fix_group_by(sql, str(e), sandbox_con, depth + 1)

