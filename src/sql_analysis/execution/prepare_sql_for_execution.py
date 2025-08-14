import re
from typing import Optional



def prepare_select_statically(sql: str) -> str:
    # ddb does not support `these` marks, replace them with "these"
    sql = sql.replace('`', '"')

    # ddb does not support "> =" and "< =", replace them with ">=" and "<="
    sql = sql.replace('> =', '>=')
    sql = sql.replace('< =', '<=')
    sql = sql.replace('! =', '!=')
    sql = sql.replace('! =', '!=')
    sql = sql.replace('= =', '=')

    # ddb date format is called 'strftime'
    pattern = r'\b(date_format|to_timestamp|to_date)\b(?=\s*\()'
    replacements = {
        'date_format': 'strftime',
        'to_timestamp': 'strptime',
        'to_date': 'strptime'
    }

    sql = re.sub(pattern, lambda m: replacements[m.group(1)], sql)

    # replace the 'yyyy - mm - dd h:m:s' to '%Y - %m - %d %H:%M:%S'

    # Replace MySQL-style LIMIT X, Y with LIMIT Y OFFSET X
    def replace_limit(match):
        offset = match.group(1).strip()
        limit = match.group(2).strip()
        return f'limit {limit} offset {offset}'

    sql = re.sub(r'limit\s+(\d+)\s*,\s*(\d+)', replace_limit, sql, flags=re.IGNORECASE)

    # Replace MySQL-style RAND() with RANDOM()
    sql = re.sub(r'\brand\s*\(\s*\)', 'random()', sql, flags=re.IGNORECASE)

    # the parser cannot handle %s placeholders, replace them with :param_name
    sql = sql.replace("%s", ":param_name")
    sql = sql.replace("%i", ":param_name")
    sql = sql.replace("%d", ":param_name")

    # the parser can't handle #{param_name}, replace it with :param_name
    sql = re.sub(r'#\{(\w+)\}', r':\1', sql)

    # the paser can't handle python f-strings like f"SELECT {param_name}", replace it with :param_name
    sql = re.sub(r'\{\s*(\w+)\s*\}', r':\1', sql)

    return sql


def escape_for_insert(sql: Optional[str]) -> Optional[str]:
    if sql is None:
        return None
    sql.replace('"', "'")  # Replace double quotes with single quotes for SQL compatibility
    # Escape single quotes by replacing them with two single quotes
    return sql.replace("'", "''")
