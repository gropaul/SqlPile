from typing import List, Dict

from src.sql_analysis.plan_analysis.models import ExpressionInfo, JoinConditionInfo


def to_list(value: List[any] | any) -> List[any]:
    if isinstance(value, list):
        return value
    return [value]


def to_expressions(expressions: List[Dict] | Dict) -> List[ExpressionInfo]:
    """
    Convert a list of expressions to a list of ExpressionInfo objects.
    """
    expressions = to_list(expressions)
    return [ExpressionInfo.from_dict(expr) for expr in expressions]


def to_conditions(expressions: List[Dict] | Dict) -> List[JoinConditionInfo]:
    """
    Convert a list of expressions to a list of ExpressionInfo objects, specifically for conditions.
    """
    expressions = to_list(expressions)
    return [JoinConditionInfo.from_dict(expr) for expr in expressions]
