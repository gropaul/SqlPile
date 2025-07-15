from typing import Literal, List, Optional
import duckdb
import re

from src.sql_analysis.execution.models import Column
from src.sql_analysis.tools.sql_types import BaseType, unify_type

OperatorType = Literal['PROJECTION', 'FILTER', 'COMPARISON_JOIN', 'AGGREGATE', 'ORDER_BY', 'SEQ_SCAN']  # type: ignore
ColumnUsageType = Literal[
    'PROJECTION', 'FILTER', 'JOIN_KEY', 'GROUP_KEY', 'AGGREGATE', 'ORDER_KEY', 'TOP_N_KEY', 'SCAN_FILTER', 'SCAN_LOOKUP']  # type: ignore

BOUND_COLUMN_REF_NAME = 'BOUND_REF'

class JoinConditionInfo:

    def __init__(self, left: 'ExpressionInfo', right: 'ExpressionInfo', comparison: str):
        self.left = left
        self.right = right
        self.comparison = comparison

    def __repr__(self):
        return f"JoinConditionInfo(left={self.left}, right={self.right}, comparison='{self.comparison}')"


    @staticmethod
    def from_dict(data: dict) -> 'JoinConditionInfo':
        left = ExpressionInfo.from_dict(data['left']) if 'left' in data else None
        right = ExpressionInfo.from_dict(data['right']) if 'right' in data else None
        comparison = data.get('comparison', '')

        return JoinConditionInfo(
            left=left,
            right=right,
            comparison=comparison
        )

class ExpressionInfo:
    def __init__(self, expression: str, expression_type: str, expression_class: str, return_type: str,
                 children: Optional[List['ExpressionInfo']] = None, binding: Optional['TableColumnBinding'] = None):
        self.expression = expression
        self.expression_type = expression_type
        self.expression_class = expression_class
        self.return_type = return_type
        self.binding: Optional['TableColumnBinding'] = binding
        self.children: List['ExpressionInfo'] = children if children is not None else []

    def __repr__(self):
        return f"ExpressionInfo(expression='{self.expression}', expression_type='{self.expression_type}', " \
               f"expression_class='{self.expression_class}', return_type='{self.return_type}', " \
               f"binding={self.binding}, " \
               f"children={self.children})"

    @staticmethod
    def from_dict(data: dict) -> 'ExpressionInfo':
        binding = None

        if 'table_index' in data or 'column_index' in data:
            binding = TableColumnBinding(
                table_id=data['table_index'] if 'table_index' in data else -1,
                column_id=data['column_index'] if 'column_index' in data else -1
            )

        return ExpressionInfo(
            expression=data.get('expression', ''),
            expression_type=data.get('expression_type', ''),
            expression_class=data.get('expression_class', ''),
            return_type=data.get('return_type', ''),
            binding=binding,
            children=[
                ExpressionInfo.from_dict(child) for child in data.get('children', [])
            ]
        )


class TableColumnBinding:
    def __init__(self, table_id: int, column_id: int):
        self.table_id: int = table_id
        self.column_id: int = column_id

    @staticmethod
    def empty() -> 'TableColumnBinding':
        """
        Returns an empty TableColumnBinding with table_id and column_id set to -1.
        This is useful for cases where no binding is available.
        """
        return TableColumnBinding(table_id=-1, column_id=-1)

    def __eq__(self, other):
        if not isinstance(other, TableColumnBinding):
            return False
        return self.table_id == other.table_id and self.column_id == other.column_id

    def __repr__(self):
        return f"TableColumnBinding(table_id={self.table_id}, column_id={self.column_id})"


class ColumnWithBinding(Column):

    def __init__(self, column_id: int, column_name: str, column_base_type: BaseType, table_binding: TableColumnBinding):
        super().__init__(column_id, column_name, column_base_type)
        self.table_binding = table_binding

    @staticmethod
    def from_column_and_binding_expression(column: Column, expression: ExpressionInfo) -> 'ColumnWithBinding':
        assert expression.expression_type == BOUND_COLUMN_REF_NAME

        assert expression.table_index is not None, "Table index must be provided for binding"
        assert expression.column_index is not None, "Column index must be provided for binding"

        table_binding = TableColumnBinding(
            table_id=expression.table_index,
            column_id=expression.column_index
        )
        return ColumnWithBinding(
            column_id=column.column_id,
            column_name=column.column_name,
            column_base_type=column.column_base_type,
            table_binding=table_binding
        )

    @staticmethod
    def from_column_and_binding(column: Column, table_id: int, column_id: int) -> 'ColumnWithBinding':
        table_binding = TableColumnBinding(
            table_id=table_id,
            column_id=column_id
        )
        return ColumnWithBinding(
            column_id=column.column_id,
            column_name=column.column_name,
            column_base_type=column.column_base_type,
            table_binding=table_binding
        )

    def __repr__(self):
        return f"ColumnWithBinding(id={self.column_id}, name='{self.column_name}', base_type='{self.column_base_type}', table_binding={self.table_binding})"


class ColumnTrack:
    def __init__(self, scanned_columns: List[Column], expression: ExpressionInfo, parents: List['ColumnTrack'],
                 base_type: BaseType, binding: TableColumnBinding):
        self.scanned_columns = scanned_columns
        self.expression = expression
        self.parents = parents
        self.base_type: BaseType = base_type
        self.binding: TableColumnBinding = binding

    @staticmethod
    def get_boolean_track() -> 'ColumnTrack':
        """
        Returns a ColumnTrack that represents a boolean track used in mark joins
        """
        return ColumnTrack(
            scanned_columns=[],
            expression=ExpressionInfo(expression='', expression_type='Boolean', expression_class='CONSTANT',
                                      return_type='Boolean'),
            parents=[],
            base_type='Boolean',
            binding=TableColumnBinding.empty()
        )

    @staticmethod
    def get_const_track(expressionInfo: ExpressionInfo) -> 'ColumnTrack':
        """
        Returns a ColumnTrack that represents a constant track.
        This is used for constant expressions in the query.
        """
        return ColumnTrack(
            scanned_columns=[],
            expression=expressionInfo,
            parents=[],
            base_type=unify_type(expressionInfo.return_type)[1],
            binding=TableColumnBinding.empty()
        )

    def __repr__(self):
        return f"ColumnTrack(involved_columns={[column.column_name for column in self.scanned_columns]}, " \
               f"expression='{self.expression}', base_type='{self.base_type}', " \
               f"binding={self.binding})"

    def get_all_involved_column_ids(self) -> List[int]:
        """
        Returns a list of all column IDs involved in this track.
        """
        # get all columns from parents and scanned columns
        involved_columns = set()
        for parent in self.parents:
            involved_columns.update(parent.get_all_involved_column_ids())
        for column in self.scanned_columns:
            involved_columns.add(column.column_id)
        return sorted(list(involved_columns))


def get_column_indices_references(expression: str) -> List[int]:
    """
    Extracts all column indices referenced in the expression.
    The indices are expected to be in the format '#<index>'.
    """
    index_pattern = re.compile(r'#(\d+)')
    index_matches = index_pattern.findall(expression)
    indices = [int(match) for match in index_matches]
    # sort the indices to ensure they are in order
    indices.sort()
    return indices


class ColumnTrackExpressionMatch:
    def __init__(self, matched_tracks: List[ColumnTrack], expression: ExpressionInfo):
        self.matched_tracks = matched_tracks
        self.expression = expression


class ColumnUsage:
    def __init__(self, query_id: int, node_id: str, column_ids: List[int], expression: str, expression_result_type: str,
                 usage_type: ColumnUsageType):
        self.query_id = query_id
        self.node_id = node_id
        self.column_ids = column_ids
        self.expression = expression
        self.expression_result_type = expression_result_type
        self.usage_type = usage_type

    @staticmethod
    def from_column_track(
            match: ColumnTrack,
            query_id: int,
            node_id: str,
            usage_type: ColumnUsageType,
    ) -> 'ColumnUsage':
        return ColumnUsage(
            query_id=query_id,
            node_id=node_id,
            column_ids=match.get_all_involved_column_ids(),
            expression=match.expression.expression,
            expression_result_type=match.base_type,
            usage_type=usage_type,
        )

    def __repr__(self):
        return f"ColumnUsage(query_id={self.query_id}, column_ids={self.column_ids}, " \
               f"expression='{self.expression}', expression_result_type='{self.expression_result_type}', " \
               f"usage_type='{self.usage_type}')"
