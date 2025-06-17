import re
import re


def split_sql_statements(sql: str) -> list[str]:
    statements = []
    current = []
    in_single_quote = False
    in_double_quote = False

    for char in sql:
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote

        if char == ';' and not in_single_quote and not in_double_quote:
            statement = ''.join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)

    # Add final part
    final = ''.join(current).strip()
    if final:
        statements.append(final)

    return statements



def tidy_up_query(sql: str) -> str:
    # Remove single-line and multi-line comments
    sql = re.sub(r'--.*?(\n|$)', ' ', sql)                   # Single-line comments
    sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)    # Multi-line comments

    # Collapse all whitespace
    sql = re.sub(r'\s+', ' ', sql)

    # Add spacing around operators and punctuation for clarity
    sql = re.sub(r'\s*([(),=<>+\-*/])\s*', r' \1 ', sql)

    # Final collapse to fix spacing artifacts
    sql = re.sub(r'\s+', ' ', sql).strip()

    # Convert to lowercase
    return sql.lower()
