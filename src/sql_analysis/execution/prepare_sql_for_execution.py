import re
from typing import Optional

prepare_sql_statically_macro = """
CREATE OR REPLACE MACRO prepare_select_statically(sql) AS
    sql
    -- backticks → double quotes
    .replace('`', '"')
    -- fix spaced comparisons
    .replace('> =', '>=')
    .replace('< =', '<=')
    .replace('! =', '!=')
    .replace('= =', '=')
    -- transform quoted date: 'YYYY - MM - DD' → 'YYYY-MM-DD'
    .regexp_replace('''(\d{4})\s*-\s*(\d{2})\s*-\s*(\d{2})''', '''\\1-\\2-\\3''', 'g')
    
    -- transform quoted timestamp: 'YYYY - MM - DD HH:MM:SS' → 'YYYY-MM-DD HH:MM:SS'
    .regexp_replace('''(\d{4})\s*-\s*(\d{2})\s*-\s*(\d{2})\s+(\d{2}:\d{2}:\d{2})''', '''\\1-\\2-\\3 \\4''', 'g')
    
    -- remove JOIN FETCH clauses: from owner left join fetch owner.pets -> from owner left join pets
.regexp_replace('(?i)(left|right|inner|outer)?\s*join\s+fetch\s+\w+\.(\w+)', '\\1 join \\2', 'g')

    -- function renames
    .replace('date_format', 'strftime')
    -- LIMIT a, b → LIMIT b OFFSET a
    .regexp_replace('(?i)limit\\s+(\\d+)\\s*,\\s*(\\d+)',
                    'limit \\2 offset \\1',
                    'g')
    -- RAND() → RANDOM()
    .regexp_replace('(?i)\\brand\\s*\\(\\s*\\)', 'random()', 'g')
    -- %s, %i, %d → :param_name
    .regexp_replace('%[sid]', ':param_name', 'g')
    -- #{param} → :param
    .regexp_replace('#\\{(\\w+)\\}', ':\\1', 'g')
    -- {param} (f‑string style) → :param
    .regexp_replace('\\{\\s*(\\w+)\\s*\\}', ':\\1', 'g')
;
"""


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
    sql = sql.replace('date_format', 'strftime')

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
    # Escape single quotes by replacing them with two single quotes
    return sql.replace("'", "''")
