import json
from typing import List, Dict, Tuple

from src.config import logger
from src.sql_analysis.execution.models import Table
from src.sql_analysis.plan_analysis.models import ColumnUsage, OperatorType, \
    ColumnTrack, ExpressionInfo, TableColumnBinding, ColumnUsageType, JoinConditionInfo
from src.sql_analysis.plan_analysis.tracking import track_and_find_usage
from src.sql_analysis.plan_analysis.utils import to_list, to_expressions, to_conditions


def get_one_child_tracks(children_tracks: List[List[ColumnTrack]]) -> List[ColumnTrack]:
    # expect tracks from one children node
    if len(children_tracks) != 1:
        logger.error(f"Expected exactly one set of children tracks, got {len(children_tracks)}.")
        return []

    children_tracks = children_tracks[0]
    return children_tracks

def get_two_child_tracks(children_tracks: List[List[ColumnTrack]]) -> Tuple[List[ColumnTrack], List[ColumnTrack]]:
    # expect tracks from two children nodes
    if len(children_tracks) != 2:
        logger.error(f"Expected exactly two sets of children tracks, got {len(children_tracks)}.")
        return [], []

    child1_tracks = children_tracks[0]
    child2_tracks = children_tracks[1]
    return child1_tracks, child2_tracks


# query_id: int, plan: Dict, tables: List[Table], children_tracks: List[ColumnTrack]
class Params:
    def __init__(self, query_id: int, plan: any, tables: List[Table], children_tracks: List[List[ColumnTrack]]):
        self.query_id = query_id
        self.plan = plan
        self.tables = tables
        self.children_tracks = children_tracks


def analyze_node(query_id: int, plan: Dict, tables: List[Table]) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a single node in the execution plan.
    """
    node_operator: OperatorType = plan['operator_type'].strip().upper()

    children_tracks: List[List[ColumnTrack]] = []
    results: List[ColumnUsage] = []

    for child in plan.get('children', []):
        child_results, child_tracks = analyze_node(query_id, child, tables)
        results.extend(child_results)
        children_tracks.append(child_tracks)

    analyze_map = {
        'GET': analyze_get,
        'PROJECTION': analyze_projection,
        'ORDER_BY': analyze_order_by,
        'FILTER': analyze_filter,
        'AGGREGATE': analyze_aggregate,
        'COMPARISON_JOIN': analyze_join,
    }

    analyze_fn = analyze_map.get(node_operator)

    if analyze_fn:
        params = Params(query_id, plan, tables, children_tracks)
        node_results, node_tracks = analyze_fn(params)
        results.extend(node_results)
        return results, node_tracks
    else:
        logger.warning(f"Unknown operator type: {node_operator}, skipping analysis.")
        return results, []


def analyze_get(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks
    extra_info = plan.get('extra_info', {})
    filter_expressions: List[str] = to_list(extra_info.get('Filters', []))
    projections: List[ExpressionInfo] = to_expressions(plan.get('expressions', []))

    # if the table is e.g. a schema table, we won't find Table
    if 'Table' not in extra_info or not extra_info['Table']:
        logger.error("No table name found in extra_info. Cannot analyze GET node.")
        return [], []

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
                    scanned_columns=[column],
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


def analyze_projection(params: Params, usage_type: ColumnUsageType = 'PROJECTION') -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks
    extra_info = plan.get('extra_info', {})
    projections: List[ExpressionInfo] = to_expressions(plan.get('expressions', []))

    children_tracks = get_one_child_tracks(children_tracks)

    usages = []
    my_tracks = []

    table_id = plan['table_index'][0]
    for (column_index, projection) in enumerate(projections):
        # check if there are any children tracks that match the projection
        binding = TableColumnBinding(table_id=table_id, column_id=column_index)
        track, usage = track_and_find_usage(projection, query_id, children_tracks, usage_type, binding=binding)
        my_tracks.append(track)
        usages.append(usage)

    # the number of tracks and usages should be the same as the number of projections
    if len(my_tracks) != len(projections):
        logger.error(
            f"Number of tracks ({len(my_tracks)}) does not match number of projections ({len(projections)}) for table with ID {table_id}.")

    return usages, my_tracks


def analyze_order_by(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks

    children_tracks = get_one_child_tracks(children_tracks)

    usages = []
    orders = to_expressions(plan.get('orders', []))
    for (column_index, order) in enumerate(orders):
        binding = TableColumnBinding(table_id=-1, column_id=column_index)
        track, usage = track_and_find_usage(order, query_id, children_tracks, 'ORDER_KEY', binding=binding)
        usages.append(usage)

    return usages, children_tracks


def analyze_filter(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a filter node in the execution plan.
    """
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks
    filter_expressions: List[ExpressionInfo] = to_expressions(plan.get('expressions', []))

    children_tracks = get_one_child_tracks(children_tracks)

    usages = []

    for (column_index, filter_expression) in enumerate(filter_expressions):
        binding = TableColumnBinding(table_id=-1, column_id=column_index)
        track, usage = track_and_find_usage(filter_expression, query_id, children_tracks,
                                            'FILTER', binding=binding)
        usages.append(usage)

    return usages, children_tracks


def analyze_aggregate(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze an aggregate node in the execution plan.

    Column ordering in DuckDB's aggregate nodes:
    Example: SELECT id, COUNT(*), MIN(id) FROM my_table GROUP BY id

    1. groups → Columns used for grouping (e.g., `id`)
    2. expressions → Aggregation expressions (e.g., `COUNT(*)`, `MIN(id)`)
    3. grouping_functions → (Unknown usage — see DuckDB source)

    For reference, see the DuckDB source code:
    [DuckDB logical_aggregate.cpp (lines 15–24)](https://github.com/duckdb/duckdb/blob/1fe72eca288f726f90103616fa6f23c057caf22a/src/planner/operator/logical_aggregate.cpp#L15-L24)
    """

    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks

    children_tracks = get_one_child_tracks(children_tracks)

    usages = []
    my_tracks = []

    groups = to_expressions(plan.get('groups', []))

    for (column_index, group_expression) in enumerate(groups):
        binding = TableColumnBinding(table_id=-1, column_id=column_index)
        track, usage = track_and_find_usage(group_expression, query_id, children_tracks,
                                            'GROUP_KEY', binding=binding)
        usages.append(usage)
        my_tracks.append(track)

    expression_usages, expression_tracks = analyze_projection(params, usage_type='AGGREGATE')
    usages.extend(expression_usages)
    my_tracks.extend(expression_tracks)

    return usages, children_tracks


def get_join_projections(plan: Dict) -> Tuple[List[int], List[int]]:
    left_p_string = plan['extra_info'].get('left_projection_map', '')
    left_p = json.loads(left_p_string) if left_p_string else []
    right_p_string = plan['extra_info'].get('right_projection_map', '')
    right_p = json.loads(right_p_string) if right_p_string else []
    return left_p, right_p

def analyze_join(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a join node in the execution plan.
    """
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks

    left_tracks, right_tracks = get_two_child_tracks(children_tracks)

    usages = []

    # Join keys are usually in the 'expressions' field
    conditions: List[JoinConditionInfo] = to_conditions(plan.get('join_conditions', []))

    for condition in conditions:
        left_expression = condition.left
        right_expression = condition.right

        _, left_usage = track_and_find_usage(left_expression, query_id, left_tracks,
                                                      'JOIN_KEY')
        _, right_usage = track_and_find_usage(right_expression, query_id, right_tracks,
                                                        'JOIN_KEY')

        # self, query_id: int, column_ids: List[int], expression: str, expression_result_type: str,
        # usage_type: ColumnUsageTyp
        combined_usage = ColumnUsage(
            query_id=query_id,
            column_ids=[*left_usage.column_ids, *right_usage.column_ids],
            expression=f"{left_expression.expression} {condition.comparison} {right_expression.expression}",
            expression_result_type=left_expression.return_type,  # Assuming both sides have the same type
            usage_type='JOIN_KEY'
        )

        usages.append(combined_usage)

    left_projections, right_projections = get_join_projections(plan)
    # if the max projection index is larger then the number of tracks, log an error
    if left_projections:
        if max(left_projections) >= len(left_tracks):
            logger.error(f"Left projections {left_projections} exceed the number of left tracks {len(left_tracks)}.")
            left_projections = [index for index in left_projections if index < len(left_tracks)]
        left_tracks = [left_tracks[index] for index in left_projections]
    if right_projections:
        if max(right_projections) >= len(right_tracks):
            logger.error(f"Right projections {right_projections} exceed the number of right tracks {len(right_tracks)}.")
            right_projections = [index for index in right_projections if index < len(right_tracks)]
        right_tracks = [right_tracks[index] for index in right_projections]

    my_tracks = [*left_tracks, *right_tracks]

    return usages, my_tracks
