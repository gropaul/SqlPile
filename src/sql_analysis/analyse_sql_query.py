from typing import Literal, Dict, Optional, List

from tree_sitter_language_pack import get_parser
from tree_sitter import Node

# ---------------------------------------------------------------------------
# Public API types
# ---------------------------------------------------------------------------

OperatorType = Literal["SELECT", "WHERE", "JOIN", "GROUP BY", "ORDER BY"]


class ColumnExpressionInfo:  # noqa: D401
    """Represents how a column participates in a binary expression (>, =, <, etc.)."""

    def __init__(self, operator: str, other_operand: str) -> None:  # noqa: D401
        self.operator = operator.lower() # e.g., ">", "=", "<", etc.
        self.other_operand = other_operand  # literal or other column

    def __repr__(self) -> str:  # noqa: D401
        return f"Expr({self.operator} {self.other_operand})"


class ColumnUsageInfo:
    """Information about a column (optionally qualified by table alias)."""

    def __init__(
        self,
        table_name: Optional[str],
        column_name: str,
        operator_type: OperatorType,
    ) -> None:  # noqa: D401,E501
        self.table_name = table_name
        self.column_name = column_name
        self.operator_type = operator_type
        self.expression: Optional[ColumnExpressionInfo] = None  # set later when discovered

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------
    def __repr__(self) -> str:  # noqa: D401
        tbl = self.table_name if self.table_name else "∅"
        expr_repr = f", expr={self.expression}" if self.expression else ""
        return (
            f"ColumnUsageInfo(table={tbl}, column={self.column_name}, "
            f"usage_place={self.operator_type}{expr_repr})"
        )


class QueryAnalysisResult:
    """Container for high‑level metrics and column usage information."""

    def __init__(
        self,
        n_selects: int,
        n_joins: int,
        n_conditions: int,
        n_group_bys: int,
        n_order_bys: int,
        column_usages: Dict[str, ColumnUsageInfo],
    ) -> None:
        self.n_selects = n_selects
        self.n_joins = n_joins
        self.n_conditions = n_conditions
        self.n_group_bys = n_group_bys
        self.n_order_bys = n_order_bys
        self.column_usages = column_usages

    def __repr__(self) -> str:  # noqa: D401
        return (
            "QueryAnalysisResult("  # noqa: E501
            f"n_selects={self.n_selects}, n_joins={self.n_joins}, n_conditions={self.n_conditions}, "
            f"n_group_bys={self.n_group_bys}, n_order_bys={self.n_order_bys}, "
            f"column_usages={list(self.column_usages.values())})"
        )


# ---------------------------------------------------------------------------
# Debug helper
# ---------------------------------------------------------------------------

def print_recursive(node: Node, indent: int = 0) -> None:
    """Pretty‑print the Tree‑sitter parse tree for debugging purposes."""

    text = ""
    if node.text:
        try:
            text = node.text.decode("utf-8")
        except UnicodeDecodeError:
            text = str(node.text)

    print(" " * indent + f"{node.type} ({node.start_point}, {node.end_point}) - {text}")

    for child in node.children:
        print_recursive(child, indent + 2)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Map grammar node types -> our canonical operator names
_OPERATOR_NODE_TYPES: Dict[str, OperatorType] = {
    "select": "SELECT",
    "where": "WHERE",
    "join": "JOIN",
    "join_clause": "JOIN",
    "group_by": "GROUP BY",
    "groupby_clause": "GROUP BY",
    "order_by": "ORDER BY",
    "orderby_clause": "ORDER BY",
}


def _nearest_operator(ancestors: List[Node]) -> Optional[OperatorType]:
    """Return the closest ancestor that corresponds to a logical operator."""

    for anc in reversed(ancestors):  # closest ancestor first
        op = _OPERATOR_NODE_TYPES.get(anc.type)
        if op:
            return op
    return None


def _field_key(table_name: Optional[str], column_name: str) -> str:
    return f"{table_name}.{column_name}" if table_name else column_name


# ---------------------------------------------------------------------------
# Core traversal logic
# ---------------------------------------------------------------------------

def analyse_node(root: Node) -> QueryAnalysisResult:
    """Traverse the parse tree and collect statistics + column usages."""

    counts: Dict[OperatorType, int] = {
        "SELECT": 0,
        "JOIN": 0,
        "WHERE": 0,  # WHERE itself isn't counted, conditions inside are.
        "GROUP BY": 0,
        "ORDER BY": 0,
    }
    n_conditions = 0
    column_usages: Dict[str, ColumnUsageInfo] = {}

    # ------------------------------------------------------------------
    # Helpers to attach expression info to ColumnUsageInfo
    # ------------------------------------------------------------------
    def _register_field(node: Node, ancestors: List[Node]):
        raw = node.text.decode("utf-8")
        if "." in raw:
            table_name, column_name = raw.split(".", 1)
        else:
            table_name = None
            column_name = raw

        usage_place = _nearest_operator(ancestors)
        key = _field_key(table_name, column_name)

        # Default placeholder if not seen before
        if key not in column_usages:
            column_usages[key] = ColumnUsageInfo(table_name, column_name, usage_place or "SELECT")
        elif usage_place and column_usages[key].operator_type == "SELECT":
            # promote more specific (e.g. WHERE over SELECT)
            column_usages[key].operator_type = usage_place

        return column_usages[key]

    # ------------------------------------------------------------------
    # Depth‑first traversal
    # ------------------------------------------------------------------
    def traverse(node: Node, ancestors: List[Node]) -> None:  # noqa: D401
        nonlocal n_conditions

        # Operator occurrence counts (except WHERE)
        op = _OPERATOR_NODE_TYPES.get(node.type)
        if op and op != "WHERE":
            counts[op] += 1

        # Handle binary expressions for conditions and column‑expression mapping
        if node.type == "binary_expression":
            # increment condition count if inside a WHERE
            if any(a.type == "where" for a in ancestors):
                n_conditions += 1

            children = list(node.children)
            if len(children) >= 3:
                lhs, operator_token, rhs = children[0], children[1], children[2]
                operator_text = operator_token.text.decode("utf-8") if operator_token.text else ""

                # If lhs is field, attach expression info
                if lhs.type == "field":
                    col_info = _register_field(lhs, ancestors)
                    other_operand_text = rhs.text.decode("utf-8") if rhs.text else rhs.type
                    col_info.expression = ColumnExpressionInfo(operator_text, other_operand_text)

                # If rhs is a field, attach expression info (mirrored)
                if rhs.type == "field":
                    col_info = _register_field(rhs, ancestors)
                    other_operand_text = lhs.text.decode("utf-8") if lhs.text else lhs.type
                    # If expression already set, keep the first one
                    if col_info.expression is None:
                        # Use the *mirrored* operator (e.g., 5 > a.age  => a.age < 5)
                        mirrored_op = {
                            ">": "<",
                            "<": ">",
                            ">=": "<=",
                            "<=": ">=",
                        }.get(operator_text, operator_text)
                        col_info.expression = ColumnExpressionInfo(mirrored_op, other_operand_text)

        # Capture field occurrences outside binary expressions
        if node.type == "field":
            _register_field(node, ancestors)

        # Recurse into children
        for child in node.children:
            traverse(child, ancestors + [node])

    traverse(root, [])

    return QueryAnalysisResult(
        n_selects=counts["SELECT"],
        n_joins=counts["JOIN"],
        n_conditions=n_conditions,
        n_group_bys=counts["GROUP BY"],
        n_order_bys=counts["ORDER BY"],
        column_usages=column_usages,
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def analyse_sql_query(sql_query: str) -> QueryAnalysisResult:  # noqa: D401
    """Analyse *sql_query* and return a :class:`QueryAnalysisResult`."""

    parser = get_parser("sql")
    tree = parser.parse(sql_query.encode("utf-8"))
    root = tree.root_node

    # Uncomment to debug parse tree:
    # print_recursive(root)

    return analyse_node(root)


# ---------------------------------------------------------------------------
# Self‑test when run directly
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    simple_like_query = """
        SELECT name FROM users WHERE name Like '%John%';
    """

    left_join_query = """
        SELECT a.name, b.age
        FROM users a
        LEFT JOIN profiles b ON a.id = b.user_id
        WHERE a.age > 30;
    """


    sample_join_query = """
        SELECT a.name, b.age
        FROM users a
        JOIN profiles b ON a.id = b.user_id
        WHERE a.age > 30;
    """

    real_query = (
        "SELECT DISTINCT emp.first_name, emp.last_name, "
        "concat(mgr.first_name, ' ', mgr.last_name) AS manager_name, "
        "emp.id, emp.manager_id, roles.title, dept.name AS department "
        "FROM employees AS emp "
        "INNER JOIN roles ON emp.role_id = roles.id "
        "INNER JOIN departments AS dept ON roles.department_id = dept.id "
        "LEFT JOIN employees AS mgr ON emp.manager_id = mgr.id "
        "WHERE emp.id = ? AND a.age > 30;"
    )

    for label, q in [
        ("Simple LIKE query", simple_like_query),
        ("Sample JOIN query", sample_join_query),
        ("Realistic complex query", real_query),
        ("Left JOIN query", left_join_query),
    ]:
        print(f"--- {label} ---")
        result = analyse_sql_query(q)
        print(result)
        print()
