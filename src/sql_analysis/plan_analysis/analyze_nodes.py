import json
from typing import List, Dict, Tuple, Optional

import duckdb

from src.config import logger
from src.sql_analysis.execution.models import Table
from src.sql_analysis.plan_analysis.models import ColumnUsage, OperatorType, \
    ColumnTrack, ExpressionInfo, TableColumnBinding, ColumnUsageType, JoinConditionInfo
from src.sql_analysis.plan_analysis.tracking import track_and_find_usage, get_column_bindings
from src.sql_analysis.plan_analysis.utils import to_list, to_expressions, to_conditions
from src.sql_analysis.tools.sql_to_schema import clean_identifier


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


class CTEOccurrence:
    """
    Represents an occurrence of a CTE in the execution plan.
    """

    def __init__(self, cte_id: int, cte_tracks: List[ColumnTrack]):
        self.cte_id = cte_id
        self.cte_tracks = cte_tracks


# query_id: int, plan: Dict, tables: List[Table], children_tracks: List[ColumnTrack]
class Params:
    def __init__(self, query_id: int, node_id: str, plan: any, tables: List[Table],
                 children_tracks: List[List[ColumnTrack]],
                 ctes: List[CTEOccurrence]):
        self.query_id = query_id
        self.node_id = node_id
        self.plan = plan
        self.tables = tables
        self.children_tracks = children_tracks
        self.occurrences: List[CTEOccurrence] = ctes if ctes is not None else []


def analyze_node(query_id: int, plan: Dict, tables: List[Table], cte_occurrence: List[CTEOccurrence],
                 parent_node_id: str = '', child_index: int = 0) -> \
        Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a single node in the execution plan.
    """

    node_operator: OperatorType = plan['operator_type'].strip().upper()

    node_id: str = f"{parent_node_id}.{child_index}" if parent_node_id else str(child_index)

    children_tracks: List[List[ColumnTrack]] = []
    results: List[ColumnUsage] = []

    for child_index, child in enumerate(plan.get('children', [])):
        child_results, child_tracks = analyze_node(query_id, child, tables, cte_occurrence, node_id, child_index)
        results.extend(child_results)
        children_tracks.append(child_tracks)

        if node_operator == 'CTE':
            # add a cte occurrence if this is a CTE node
            cte_id = plan.get('extra_info', {}).get('Table Index', -1)
            cte = CTEOccurrence(cte_id=cte_id, cte_tracks=children_tracks[0])
            existing_ids = [occ.cte_id for occ in cte_occurrence]
            if cte_id not in existing_ids:
                cte_occurrence.append(cte)

    analyze_map = {
        'GET': analyze_get,
        'CHUNK_GET': analyze_chunk_get,
        'DELIM_GET': analyze_chunk_get,
        'PROJECTION': analyze_projection,
        'ORDER_BY': analyze_order_by,
        'FILTER': analyze_filter,
        'AGGREGATE': analyze_aggregate,
        'DELIM_JOIN': analyze_join,
        'COMPARISON_JOIN': analyze_join,
        'LOGICAL_ANY_JOIN': analyze_join,
        'UNION': analyze_union,
        'TOP_N': analyze_top_n,
        'LIMIT': analyze_limit,
        'CTE': analyze_cte,
        'CTE_SCAN': analyze_cte_scan,
        'DISTINCT': analyze_distinct,
        'CROSS_PRODUCT': analyze_cross_product,
        'WINDOW': analyze_window,

    }

    analyze_fn = analyze_map.get(node_operator)

    if analyze_fn:
        params = Params(query_id, node_id, plan, tables, children_tracks, cte_occurrence)
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

    table_filters = to_expressions(plan.get('table_filters', []))

    # if the table is e.g., a schema table, we won't find Table
    if 'Table' not in extra_info or not extra_info['Table']:
        logger.error("No table name found in extra_info. Cannot analyze GET node.")
        return [], []

    scan_table_name = clean_identifier(extra_info['Table'])
    table_id = plan['table_index'][0]

    scan_table: Optional[Table] = None
    my_tracks = []
    for table in tables:
        if table.table_name.lower() == scan_table_name.lower():
            scan_table = table
            break

    if scan_table is None:
        logger.error(f"Table {scan_table_name} not found in the provided tables.")
        return [], []

    for (column_index, projection) in enumerate(projections):
        # Check if the projection is a column in the table
        for column in scan_table.columns:
            if column.column_name.lower() == projection.expression.lower():
                my_tracks.append(ColumnTrack(
                    scanned_columns=[column],
                    expression=projection,
                    base_type=column.column_base_type,
                    parents=[],
                    binding=TableColumnBinding(
                        table_id=table_id,
                        column_id=column_index
                    )
                ))

    filter_usages = []
    for (column_index, filter_expression) in enumerate(table_filters):
        expression_column_bindings = get_column_bindings(filter_expression)
        used_columns = []
        for binding in expression_column_bindings:
            for column in scan_table.columns:
                if column.column_name.lower() == binding.expression.lower():
                    used_columns.append(column.column_id)
        filter_usages.append(ColumnUsage(
            query_id=query_id,
            node_id=params.node_id,
            column_ids=used_columns,
            expression=filter_expression.expression,
            expression_result_type=filter_expression.return_type,
            usage_type='SCAN_FILTER',
        ))

    # the number of tracks and usages should be the same as the number of projections
    if len(my_tracks) != len(projections):
        logger.error(
            f"Number of tracks ({len(my_tracks)}) does not match number of projections ({len(projections)}) for table {scan_table_name}.")

    scan_usages = [
        ColumnUsage.from_column_track(
            match=track,
            query_id=query_id,
            node_id=params.node_id,
            usage_type='SCAN_LOOKUP'
        ) for track in my_tracks
    ]

    usages = scan_usages + filter_usages
    return usages, my_tracks


def analyze_chunk_get(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    params.query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks

    expressions: List[ExpressionInfo] = to_expressions(plan.get('expressions', []))

    my_tracks = [ColumnTrack.get_const_track(e) for e in expressions]

    usages = []  # it's just a chunk scan, no usage type

    return usages, my_tracks


def analyze_projection(params: Params, usage_type: ColumnUsageType = 'PROJECTION') -> Tuple[
    List[ColumnUsage], List[ColumnTrack]]:
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
        track, usage = track_and_find_usage(projection, query_id, params.node_id, children_tracks, usage_type,
                                            binding=binding)
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
        track, usage = track_and_find_usage(order, query_id, params.node_id, children_tracks, 'ORDER_KEY',
                                            binding=binding)
        usages.append(usage)


    projection = get_projection(plan, 'projection_map')
    children_tracks = map_tracks(children_tracks, projection)

    # all the values need to be materialized during ordering
    for child_track in children_tracks:
        usage = ColumnUsage.from_column_track(child_track, query_id, params.node_id, 'ORDER_MATERIALIZATION')
        usages.append(usage)

    return usages, children_tracks


def map_tracks(tracks: List[ColumnTrack], projection_map: List[int]) -> List[ColumnTrack]:
    if len(projection_map) == 0:
        return tracks
    else:
        return [tracks[index] for index in projection_map]


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
        track, usage = track_and_find_usage(filter_expression, query_id, params.node_id, children_tracks,
                                            'FILTER', binding=binding)
        usages.append(usage)

    projection = get_projection(plan, 'projection_map')
    children_tracks = map_tracks(children_tracks, projection)
    return usages, children_tracks


def analyze_aggregate(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze an aggregate node in the execution plan.

    Column ordering in DuckDB's aggregate nodes:
    Example: SELECT id, COUNT(*), MIN(id) FROM my_table GROUP BY id

    1. groups → Columns used for grouping (e.g., `id`)
    2. Expressions → Aggregation expressions (e.g., `COUNT(*)`, `MIN(id)`)
    3. Grouping_functions → (Unknown usage — see DuckDB source)

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
        track, usage = track_and_find_usage(group_expression, query_id, params.node_id, children_tracks,
                                            'GROUP_KEY', binding=binding)
        usages.append(usage)
        my_tracks.append(track)

    expression_usages, expression_tracks = analyze_projection(params, usage_type='AGGREGATE')
    usages.extend(expression_usages)
    my_tracks.extend(expression_tracks)

    return usages, my_tracks


def get_projection(plan: Dict, key: str) -> List[int]:
    string = plan['extra_info'].get(key, '')
    return json.loads(string) if string else []


def get_join_projections(plan: Dict) -> Tuple[List[int], List[int]]:
    left_p = get_projection(plan, 'left_projection_map')
    right_p = get_projection(plan, 'right_projection_map')
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

        _, left_usage = track_and_find_usage(left_expression, query_id, params.node_id, left_tracks,
                                             'JOIN_KEY')
        _, right_usage = track_and_find_usage(right_expression, query_id, params.node_id, right_tracks,
                                              'JOIN_KEY')

        # self, query_id: int, column_ids: List[int], expression: str, expression_result_type: str,
        # usage_type: ColumnUsageTyp
        combined_usage = ColumnUsage(
            query_id=query_id,
            node_id=params.node_id,
            column_ids=[*left_usage.column_ids, *right_usage.column_ids],
            expression=f"{left_expression.expression} {condition.comparison} {right_expression.expression}",
            expression_result_type=left_expression.return_type,  # Assuming both sides have the same type
            usage_type='JOIN_KEY'
        )

        usages.append(combined_usage)

    left_projections, right_projections = get_join_projections(plan)
    # if the max projection index is larger, then the number of tracks, log an error
    if left_projections:
        if max(left_projections) >= len(left_tracks):
            logger.error(f"Left projections {left_projections} exceed the number of left tracks {len(left_tracks)}.")
            left_projections = [index for index in left_projections if index < len(left_tracks)]
        left_tracks = [left_tracks[index] for index in left_projections]
    if right_projections:
        if max(right_projections) >= len(right_tracks):
            logger.error(
                f"Right projections {right_projections} exceed the number of right tracks {len(right_tracks)}.")
            right_projections = [index for index in right_projections if index < len(right_tracks)]
        right_tracks = [right_tracks[index] for index in right_projections]

    join_type = plan.get('extra_info', {}).get('Join Type', 'INNER').upper()

    # see https://github.com/duckdb/duckdb/blob/4a11bc84256b736953a490bebd9bc6ca4faf227d/src/planner/operator/logical_join.cpp#L33
    if join_type in ['SEMI', 'ANTI']:
        # For SEMI and ANTI joins, we only project the left-hand side
        return usages, left_tracks

    if join_type == 'MARK':
        # For MARK join, we project the left-hand side, plus a BOOLEAN column indicating the MARK
        left_tracks.append(ColumnTrack.get_boolean_track())
        return usages, left_tracks

    if join_type in ['RIGHT_SEMI', 'RIGHT_ANTI']:
        # For RIGHT_SEMI and RIGHT_ANTI joins, we project the right-hand side
        return usages, right_tracks

    # the right tracks need to be materialized
    for right_track in right_tracks:
        usage = ColumnUsage.from_column_track(right_track, query_id, params.node_id, 'JOIN_MATERIALIZATION')
        usages.append(usage)

    # For any other join, we project both sides
    return usages, left_tracks + right_tracks


def analyze_union(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a union node in the execution plan. Return the tracks from the children nodes.
    """

    child_1, child_2 = get_two_child_tracks(params.children_tracks)

    # they result is are the tracks of the first child, see
    # https://github.com/duckdb/duckdb/blob/5e6dbcb8ad3d7d393e9d2668ed3cf61be802506d/src/include/duckdb/planner/operator/logical_set_operation.hpp#L56
    usages = []
    return usages, child_1


def analyze_top_n(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a TOP_N node in the execution plan.
    """
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks

    children_tracks = get_one_child_tracks(children_tracks)

    usages = []
    orders = to_expressions(plan.get('orders', []))
    for (column_index, order) in enumerate(orders):
        binding = TableColumnBinding(table_id=-1, column_id=column_index)
        track, usage = track_and_find_usage(order, query_id, params.node_id, children_tracks,
                                            'TOP_N_KEY', binding=binding)
        usages.append(usage)

    return usages, children_tracks


def analyze_limit(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a LIMIT node in the execution plan.
    """
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks
    children_tracks = get_one_child_tracks(children_tracks)
    usages = []

    return usages, children_tracks


def analyze_cte(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a CTE (Common Table Expression) node in the execution plan.
    """
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks

    # CTEs are usually just projections of the child node
    left, right = get_two_child_tracks(children_tracks)

    return [], right


def analyze_cte_scan(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a CTE_SCAN node in the execution plan.
    """
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks

    # CTE_SCAN is a projection of the CTE node
    cte_id = plan.get('extra_info', {}).get('CTE Index', -1)

    cte_occurrence = next((occ for occ in params.occurrences if occ.cte_id == cte_id), None)
    if cte_occurrence is None:
        logger.error(f"CTE with ID {cte_id} not found in occurrences.")
        return [], []

    cte_tracks = cte_occurrence.cte_tracks

    return [], cte_tracks


def analyze_distinct(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a DISTINCT node in the execution plan.
    """
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks

    children_tracks = get_one_child_tracks(children_tracks)
    usages = []

    distinct_targets = to_expressions(plan.get('distinct_targets', []))

    for (column_index, target) in enumerate(distinct_targets):
        _, usage = track_and_find_usage(target, query_id, params.node_id, children_tracks,
                                        'DISTINCT_KEY')
        usages.append(usage)

    return usages, children_tracks


def analyze_cross_product(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks

    left_tracks, right_tracks = get_two_child_tracks(children_tracks)

    usages = []

    # CROSS_PRODUCT does not have join conditions, so we just return the tracks from both children
    return usages, left_tracks + right_tracks


def analyze_window(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks

    children_tracks = get_one_child_tracks(children_tracks)

    usages = []
    window_expressions = to_expressions(plan.get('expressions', []))

    expression_tracks = []
    for (column_index, window_expression) in enumerate(window_expressions):
        binding = TableColumnBinding(table_id=-1, column_id=column_index)
        track, usage = track_and_find_usage(window_expression, query_id, params.node_id, children_tracks,
                                            'WINDOW_EXPRESSION', binding=binding)
        usages.append(usage)
        expression_tracks.append(track)

    # window op returns the tracks from the child node and then its own expressions' return types
    return usages, children_tracks + expression_tracks
