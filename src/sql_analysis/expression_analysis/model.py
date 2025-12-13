from __future__ import annotations

from typing import TypedDict, List


class ExpressionDict(TypedDict):
    expression: str
    expression_type: str
    expression_class: str
    return_type: str
    children: List[ExpressionDict]

class ExpressionAggregateDict(TypedDict):
    expression_type: str
    expression_class: str
    next_options: List[NextOption]


class NextOption(TypedDict):
    count: int
    children: List[ExpressionAggregateDict]

