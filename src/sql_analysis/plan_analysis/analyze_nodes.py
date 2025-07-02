from typing import List, Dict, Tuple

from src.config import logger
from src.sql_analysis.execution.models import Table
from src.sql_analysis.plan_analysis.models import ColumnUsage, OperatorType, \
    ColumnTrack, ExpressionInfo, TableColumnBinding
from src.sql_analysis.plan_analysis.tracking import track_and_find_usage
from src.sql_analysis.plan_analysis.utils import to_list, to_expressions


def analyze_node(query_id: int, plan: Dict, tables: List[Table]) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a single node in the execution plan.
    """
    node_operator: OperatorType = plan['operator_type'].strip().upper()

    tracks: List[ColumnTrack] = []
    results: List[ColumnUsage] = []

    for child in plan.get('children', []):
        child_results, child_columns = analyze_node(query_id, child, tables)
        results.extend(child_results)
        tracks.extend(child_columns)

    if node_operator == 'PROJECTION':
        node_results, tracks = analyze_projection(query_id, plan, tables, tracks)
        results.extend(node_results)
    elif node_operator == 'GET':
        node_results, tracks = analyze_get(query_id, plan, tables, tracks)
        results.extend(node_results)
    elif node_operator == 'UNGROUPED_AGGREGATE':
        node_results, tracks = analyze_ungrouped_aggregation(query_id, plan, tables, tracks)
        results.extend(node_results)
    elif node_operator == 'HASH_GROUP_BY':
        node_results, tracks = analyze_grouped_aggregation(query_id, plan, tables, tracks)
        results.extend(node_results)
    else:
        logger.warning(f"Unknown operator type: {node_operator}, skipping analysis.")

    return results, tracks


def analyze_ungrouped_aggregation(query_id: int, plan: Dict, tables: List[Table], children_tracks: List[ColumnTrack]) -> \
        Tuple[
            List[ColumnUsage], List[ColumnTrack]]:
    extra_info = plan.get('extra_info', {})

    my_tracks: List[ColumnTrack] = []
    usages: List[ColumnUsage] = []

    aggregates = to_list(extra_info.get('Aggregates', []))
    for projection in aggregates:
        # check if there are any children tracks that match the projection
        track, usage = track_and_find_usage(projection, query_id, children_tracks, 'AGGREGATE')
        my_tracks.append(track)
        usages.append(usage)

    return usages, my_tracks


def analyze_grouped_aggregation(query_id: int, plan: Dict, tables: List[Table], children_tracks: List[ColumnTrack]) -> \
        Tuple[
            List[ColumnUsage], List[ColumnTrack]]:
    aggregate_usages, aggregate_tracks = analyze_ungrouped_aggregation(query_id, plan, tables, children_tracks)

    extra_info = plan.get('extra_info', {})
    groups = to_list(extra_info.get('Groups', []))
    for group in groups:
        # the groups are not returned as expressions.
        _track, usage = track_and_find_usage(group, query_id, children_tracks, 'GROUP_KEY')
        aggregate_usages.append(usage)


def analyze_get(query_id: int, plan: Dict, tables: List[Table], children_tracks: List[ColumnTrack]) -> Tuple[
    List[ColumnUsage], List[ColumnTrack]]:
    extra_info = plan.get('extra_info', {})
    filter_expressions: List[str] = to_list(extra_info.get('Filters', []))
    projections: List[ExpressionInfo] = to_expressions(plan.get('expressions', []))
    scan_table_name = extra_info['Table']

    scan_table: Table = None
    my_tracks = []
    for table in tables:
        if table.table_name == scan_table_name:
            scan_table = table
            break

    if scan_table is None:
        logger.error(f"Table {scan_table_name} not found in the provided tables.")
        return [], []

    for (column_index, projection) in enumerate(projections):
        # Check if the projection is a column in the table
        for column in scan_table.columns:
            if column.column_name.lower() in projection.expression.lower():
                my_tracks.append(ColumnTrack(
                    involved_columns=[column],
                    expression=projection,
                    base_type=column.column_base_type,
                    parents=[],
                    binding=TableColumnBinding(
                        table_id=plan['table_index'][0],
                        column_id=column_index
                    )
                ))

    # the number of tracks and usages should be the same as the number of projections
    if len(my_tracks) != len(projections):
        logger.error(
            f"Number of tracks ({len(my_tracks)}) does not match number of projections ({len(projections)}) for table {scan_table_name}.")

    usages = [
        ColumnUsage.from_column_track(
            match=track,
            query_id=query_id,
            usage_type='SCAN_LOOKUP'
        ) for track in my_tracks
    ]
    return usages, my_tracks


def analyze_projection(query_id: int, plan: Dict, tables: List[Table], children_tracks: List[ColumnTrack]) -> Tuple[
    List[ColumnUsage], List[ColumnTrack]]:
    extra_info = plan.get('extra_info', {})
    projections: List[ExpressionInfo] = to_expressions(plan.get('expressions', []))

    usages = []
    my_tracks = []

    table_id = plan['table_index'][0]
    for (column_index, projection) in enumerate(projections):
        # check if there are any children tracks that match the projection
        binding = TableColumnBinding(table_id=table_id, column_id=column_index)
        track, usage = track_and_find_usage(projection, query_id, children_tracks, 'PROJECTION', binding=binding)
        my_tracks.append(track)
        usages.append(usage)

    # the number of tracks and usages should be the same as the number of projections
    if len(my_tracks) != len(projections):
        logger.error(
            f"Number of tracks ({len(my_tracks)}) does not match number of projections ({len(projections)}) for table with ID {table_id}.")

    return usages, my_tracks
