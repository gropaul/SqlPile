from typing import List, Dict, Tuple

from src.config import logger
from src.sql_analysis.execution.models import Table
from src.sql_analysis.plan_analysis.models import ColumnUsage, OperatorType, \
    ColumnTrack, ExpressionInfo, TableColumnBinding
from src.sql_analysis.plan_analysis.tracking import track_and_find_usage
from src.sql_analysis.plan_analysis.utils import to_list, to_expressions

# query_id: int, plan: Dict, tables: List[Table], children_tracks: List[ColumnTrack]
class Params:
    def __init__(self, query_id: int, plan: any, tables: List[Table], children_tracks: List[ColumnTrack]):
        self.query_id = query_id
        self.plan = plan
        self.tables = tables
        self.children_tracks = children_tracks

def analyze_node(query_id: int, plan: Dict, tables: List[Table]) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a single node in the execution plan.
    """
    node_operator: OperatorType = plan['operator_type'].strip().upper()

    tracks: List[ColumnTrack] = []
    results: List[ColumnUsage] = []

    for child in plan.get('children', []):
        child_results, child_tracks = analyze_node(query_id, child, tables)
        results.extend(child_results)
        tracks.extend(child_tracks)

    params = Params(query_id, plan, tables, tracks)
    if node_operator == 'GET':
        node_results, tracks = analyze_get(params)
        results.extend(node_results)
    elif node_operator == 'PROJECTION':
        node_results, tracks = analyze_projection(params)
        results.extend(node_results)
    elif node_operator == 'ORDER_BY':
        node_results, tracks = analyze_order_by(params)
        results.extend(node_results)
    elif node_operator == 'FILTER':
        node_results, tracks = analyze_filter(params)  # Filter is treated like a projection
        results.extend(node_results)
    else:
        logger.warning(f"Unknown operator type: {node_operator}, skipping analysis.")

    return results, tracks


def analyze_get(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks
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


def analyze_projection(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks
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


def analyze_order_by(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:

    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks

    usages = []
    orders = to_expressions(plan.get('orders', []))
    for (column_index, order) in enumerate(orders):
        binding = TableColumnBinding(table_id=-1, column_id=column_index)
        track, usage = track_and_find_usage(order, query_id, children_tracks, 'ORDER_KEY', binding=binding)
        usages.append(usage)

    return usages, params.children_tracks

def analyze_filter(params: Params) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a filter node in the execution plan.
    """
    query_id, plan, tables, children_tracks = params.query_id, params.plan, params.tables, params.children_tracks
    filter_expressions: List[ExpressionInfo] = to_expressions(plan.get('expressions', []))

    usages = []

    for (column_index, filter_expression) in enumerate(filter_expressions):
        binding = TableColumnBinding(table_id=-1, column_id=column_index)
        track, usage = track_and_find_usage(filter_expression, query_id, children_tracks,
                                            'FILTER', binding=binding)
        usages.append(usage)

    return usages, children_tracks