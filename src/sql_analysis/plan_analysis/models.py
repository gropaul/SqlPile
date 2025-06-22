from typing import Literal, List
import duckdb
import re

from src.sql_analysis.execution.models import Column
from src.sql_analysis.tools.sql_types import BaseType, base_type_to_example_value, unify_type

OperatorType = Literal['PROJECTION', 'FILTER', 'COMPARISON_JOIN', 'AGGREGATE', 'ORDER_BY', 'SEQ_SCAN']  # type: ignore
ColumnUsageType = Literal[
    'PROJECTION', 'FILTER', 'JOIN_KEY', 'GROUP_KEY', 'AGGREGATE', 'ORDER_KEY', 'SCAN_FILTER', 'SCAN_LOOKUP']  # type: ignore


class ColumnTrack:
    def __init__(self, involved_columns: List[Column], expression: str, parents: List['ColumnTrack'],
                 base_type: BaseType):
        self.involved_columns = involved_columns
        self.expression = expression
        self.parents = parents
        self.base_type: BaseType = base_type

    def get_column_names(self) -> List[str]:
        return [column.column_name.lower() for column in self.involved_columns]

    def __repr__(self):
        return f"ColumnTrack(involved_columns={[column.column_name for column in self.involved_columns]}, " \
               f"expression='{self.expression}', base_type='{self.base_type}')"


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
    def __init__(self, matched_tracks: List[ColumnTrack], expression: str, matched_columns: List[Column]):
        self.matched_tracks = matched_tracks
        self.expression = expression
        self.matched_columns = matched_columns

    def get_expression_return_type(self) -> BaseType:
        expression_copy = self.expression.lower()

        # if the _no_overflow is in the expression, we remove it. This is used e.g. in ddb sum_no_overflow
        # but is an internal function that ddb refused to parse
        expression_copy = expression_copy.replace('_no_overflow', '')

        for track in self.matched_tracks:
            for column in track.involved_columns:
                column_base_type_example_value = base_type_to_example_value(column.column_base_type)
                expression_copy = expression_copy.replace(column.column_name.lower(), column_base_type_example_value)

        # The matches can be based on the track findings before, they are ordered.
        for index_of_index, index in enumerate(get_column_indices_references(expression_copy)):
            track = self.matched_tracks[index_of_index]
            example_value = base_type_to_example_value(track.base_type)
            expression_copy = expression_copy.replace(f'#{index}', example_value)

        query = f"SELECT {expression_copy} AS result"
        result = duckdb.query(query)
        type = result.types[0].id

        detailed_type, base_type = unify_type(type)

        return base_type


class ColumnUsage:
    def __init__(self, query_id: int, column_ids: List[int], expression: str, expression_result_type: str,
                 usage_type: ColumnUsageType):
        self.query_id = query_id
        self.column_ids = column_ids
        self.expression = expression
        self.expression_result_type = expression_result_type
        self.usage_type = usage_type

    @staticmethod
    def from_column_track(
            match: ColumnTrack,
            query_id: int,
            usage_type: ColumnUsageType,
    ) -> 'ColumnUsage':
        return ColumnUsage(
            query_id=query_id,
            column_ids=[c.column_id for c in match.involved_columns],
            expression=match.expression,
            expression_result_type=match.base_type,
            usage_type=usage_type,
        )

    def __repr__(self):
        return f"ColumnUsage(query_id={self.query_id}, column_ids={self.column_ids}, " \
               f"expression='{self.expression}', expression_result_type='{self.expression_result_type}', " \
               f"usage_type='{self.usage_type}')"