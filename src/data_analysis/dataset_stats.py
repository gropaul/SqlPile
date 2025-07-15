import duckdb



def get_operator_stats():
    """
    Create a view with the number of operators in a query
    """

    con = duckdb.connect('/Users/paul/workspace/SqlPile/data/schemapile.duckdb')
    data = con.execute("""
        WITH ops AS (
          SELECT query_id, node_id, usage_type as op
          FROM column_usages
          GROUP BY query_id, node_id, usage_type
        ),
        op_counts AS (
          SELECT query_id, op, COUNT(*) as op_count
          FROM ops
          GROUP BY query_id, op
        ),
        queries_count AS (
          SELECT COUNT(DISTINCT query_id) as query_count
          FROM op_counts
        )
        SELECT 
          oc.op,
          SUM(oc.op_count) as total_count,
          COUNT(DISTINCT oc.query_id) as queries_having_op_cnt,
          SUM(oc.op_count)::float / qc.query_count as avg_per_query
        FROM op_counts oc
        CROSS JOIN queries_count qc
        GROUP BY oc.op, qc.query_count
        ORDER BY oc.op DESC;
    """).fetchall()

    for row in data:
        print(row)
    


if __name__ == "__main__":
    get_operator_stats()