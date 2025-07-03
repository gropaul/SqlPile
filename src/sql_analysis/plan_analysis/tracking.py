from typing import List, Tuple

from src.sql_analysis.plan_analysis.models import ExpressionInfo, ColumnTrack, ColumnTrackExpressionMatch, ColumnUsage, \
    TableColumnBinding, ColumnUsageType, BOUND_COLUMN_REF_NAME
from src.sql_analysis.tools.sql_types import unify_type


def get_column_bindings(expression: ExpressionInfo) -> List[ExpressionInfo]:
    column_bindings = []
    for child in expression.children:
        child_bindings = get_column_bindings(child)
        column_bindings.extend(child_bindings)

    if expression.expression_type == BOUND_COLUMN_REF_NAME:
        column_bindings.append(expression)

    return column_bindings

def match_tracks_to_expression(expression: ExpressionInfo, tracks: List[ColumnTrack]) -> ColumnTrackExpressionMatch:
    expression_column_bindings = get_column_bindings(expression)
    bound_tracks = []


    for binding in expression_column_bindings:
        # check if the binding id exists in the tracks
        if len(tracks) <= binding.binding.column_id:
            continue
        bound_tracks.append(tracks[binding.binding.column_id])

    return ColumnTrackExpressionMatch(
        matched_tracks=bound_tracks,
        expression=expression,
    )


def track_and_find_usage(expression: ExpressionInfo, query_id: int, children_tracks: List[ColumnTrack],
                         usage: ColumnUsageType, binding: TableColumnBinding = TableColumnBinding.empty()) -> Tuple[ColumnTrack, ColumnUsage]:
    match: ColumnTrackExpressionMatch = match_tracks_to_expression(expression, children_tracks)
    _, projection_base_type = unify_type(expression.return_type)

    track = ColumnTrack(
        scanned_columns=[],
        parents=match.matched_tracks,
        expression=expression,
        base_type=projection_base_type,
        binding=binding
    )

    column_usage = ColumnUsage.from_column_track(
        match=track,
        query_id=query_id,
        usage_type=usage
    )

    return track, column_usage

