import re


def prepare_sql_for_duckdb(sql: str) -> str:
    # ddb does not support `these` marks, replace them with "these"
    sql = sql.replace('`', '"')
    # ddb does not support "> =" and "< =", replace them with ">=" and "<="
    sql = sql.replace('> =', '>=')
    sql = sql.replace('< =', '<=')
    sql = sql.replace('! =', '!=')

    # ddb date format is called 'strftime'
    sql = sql.replace('date_format', 'strftime')

    # Replace MySQL-style LIMIT X, Y with LIMIT Y OFFSET X
    def replace_limit(match):
        offset = match.group(1).strip()
        limit = match.group(2).strip()
        return f'limit {limit} offset {offset}'

    sql = re.sub(r'limit\s+(\d+)\s*,\s*(\d+)', replace_limit, sql, flags=re.IGNORECASE)

    # Replace MySQL-style RAND() with RANDOM()
    sql = re.sub(r'\brand\s*\(\s*\)', 'random()', sql, flags=re.IGNORECASE)

    return sql