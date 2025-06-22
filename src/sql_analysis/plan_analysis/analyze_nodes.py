import json
from errno import ECHILD
import re


import duckdb
from pure_eval import group_expressions

from src.config import logger
from src.sql_analysis.execution.models import Table, Column
from src.sql_analysis.plan_analysis.models import ColumnUsage, ColumnUsageType, OperatorType, \
    ColumnTrack, ColumnTrackExpressionMatch, get_column_indices_references
from typing import List, Dict, Optional, Literal, Tuple


def analyze_node(query_id: int, plan: Dict, tables: List[Table]) -> Tuple[List[ColumnUsage], List[ColumnTrack]]:
    """
    Analyze a single node in the execution plan.
    """
    node_operator: OperatorType = plan['name'].strip().upper()

    tracks: List[ColumnTrack] = []
    results: List[ColumnUsage] = []

    for child in plan.get('children', []):
        child_results, child_columns = analyze_node(query_id, child, tables)
        results.extend(child_results)
        tracks.extend(child_columns)

    if node_operator == 'PROJECTION':
        node_results, tracks = analyze_projection(query_id, plan, tables, tracks)
        results.extend(node_results)
    elif node_operator == 'SEQ_SCAN':
        node_results, tracks = analyze_seq_scan(query_id, plan, tables, tracks)
        results.extend(node_results)
    elif node_operator == 'UNGROUPED_AGGREGATE':
        node_results, tracks = analyze_ungrouped_aggregation(query_id, plan, tables, tracks)

    else:
        logger.warning(f"Unknown operator type: {node_operator}, skipping analysis.")

    return results, tracks


def to_list(value: List[any] | any) -> List[any]:
    if isinstance(value, list):
        return value
    return [value]

def analyze_ungrouped_aggregation(query_id: int, plan: Dict, tables: List[Table], children_tracks: List[ColumnTrack]) -> Tuple[
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




def analyze_seq_scan(query_id: int, plan: Dict, tables: List[Table], children_tracks: List[ColumnTrack]) -> Tuple[
    List[ColumnUsage], List[ColumnTrack]]:
    extra_info = plan.get('extra_info', {})
    filter_expressions: List[str] = to_list(extra_info.get('Filters', []))
    projections = to_list(extra_info.get('Projections', []))
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

    for projection in projections:
        # Check if the projection is a column in the table
        for column in scan_table.columns:
            if column.column_name.lower() in projection.lower():
                my_tracks.append(ColumnTrack(
                    involved_columns=[column],
                    expression=projection,
                    base_type=column.column_base_type,
                    parents=[]
                ))

    usages = [
        ColumnUsage.from_column_track(
            match=track,
            query_id=query_id,
            usage_type='SCAN_LOOKUP'
        ) for track in my_tracks
    ]
    return usages, my_tracks


def match_tracks_to_expression(expression: str, tracks: List[ColumnTrack]) -> ColumnTrackExpressionMatch:
    matched_tracks: List[ColumnTrack] = []
    matching_columns: List[Column] = []

    # sometimes the expression can have a index reference to a column, e.g. 'lower(#1)' which references the
    # track with index 1 in the projection (2nd column in the projection)
    for index in get_column_indices_references(expression):
        matched_tracks.append(tracks[index])
        matching_columns.extend(tracks[index].involved_columns)


    for track in tracks:
        columns = track.involved_columns
        matching_columns_of_track = []
        for column in columns:
            if column.column_name.lower() in expression.lower():
                matching_columns_of_track.append(column)
        if matching_columns_of_track:
            matched_tracks.append(track)
            matching_columns.extend(matching_columns_of_track)

    return ColumnTrackExpressionMatch(
        matched_tracks=matched_tracks,
        expression=expression,
        matched_columns=matching_columns
    )


def track_and_find_usage(expression: str, query_id: int, children_tracks: List[ColumnTrack], usage: ColumnUsageType) -> Tuple[ColumnTrack, ColumnUsage]:
    match = match_tracks_to_expression(expression, children_tracks)
    projection_base_type = match.get_expression_return_type()

    track = ColumnTrack(
        involved_columns=match.matched_columns,
        parents=match.matched_tracks,
        expression=expression,
        base_type=projection_base_type
    )

    column_usage = ColumnUsage.from_column_track(
        match=track,
        query_id=query_id,
        usage_type=usage
    )

    return track, column_usage

def analyze_projection(query_id: int, plan: Dict, tables: List[Table], children_tracks: List[ColumnTrack]) -> Tuple[
    List[ColumnUsage], List[ColumnTrack]]:
    extra_info = plan.get('extra_info', {})
    projections: List[str] = to_list(extra_info.get('Projections', []))

    usages = []
    my_tracks = []
    for projection in projections:
        # check if there are any children tracks that match the projection
        track, usage = track_and_find_usage(projection, query_id, children_tracks, 'PROJECTION')
        my_tracks.append(track)
        usages.append(usage)

    return usages, my_tracks


def test_projection_scan():
    con = duckdb.connect()

    # create table and insert data
    con.execute("CREATE TABLE my_table (id INTEGER, name VARCHAR)")

    con.execute("INSERT INTO my_table VALUES (1, 'Alice'), (2, 'Bob')")

    # create a mock plan
    result = con.execute("""EXPLAIN (FORMAT json) SELECT id, lower(name) FROM my_table""").fetchall()

    plan = json.loads(result[0][1])[0]
    print(plan)

    table = Table(table_id=1, table_name='my_table', columns=[
        Column(column_id=1, column_name='id', column_base_type='Int'),
        Column(column_id=2, column_name='name', column_base_type='Text')
    ])

    usages, columns = analyze_node(1, plan, [table])
    print(columns)



def test_ungrouped_aggregation():
    con = duckdb.connect()

    # create table and insert data
    con.execute("CREATE TABLE my_table (amount INTEGER, category VARCHAR)")

    con.execute("INSERT INTO my_table VALUES (100, 'A'), (200, 'B'), (150, 'A')")

    # create a mock plan
    result = con.execute("""EXPLAIN (FORMAT json) SELECT sum(amount), COUNT(amount), MIN(category) FROM my_table""").fetchall()
    plan = json.loads(result[0][1])[0]
    print(plan)
    table = Table(table_id=1, table_name='my_table', columns=[
        Column(column_id=1, column_name='amount', column_base_type='Int'),
        Column(column_id=2, column_name='category', column_base_type='Text')
    ])

    usages, columns = analyze_node(1, plan, [table])
    for column in columns:
        print(column)

    for usage in usages:
        print(usage)