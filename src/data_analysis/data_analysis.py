
from src.config import DATABASE_PATH
import duckdb
import math
from collections import Counter


def normalized_entropy(s: str) -> float:
    """Calculate the normalized Shannon entropy of a string."""
    if not s:
        return 0.0

    counts = Counter(s)
    total = len(s)
    k = len(counts)

    shannon_entropy = -sum((count / total) * math.log2(count / total) for count in counts.values())
    max_entropy = math.log2(k) if k > 1 else 1.0  # Avoid division by 0

    return shannon_entropy / max_entropy


def analysis_2():

    con = duckdb.connect(DATABASE_PATH)
    con.create_function("normalized_entropy", normalized_entropy, [str], float, type="native")

    view_query = """
        CREATE OR REPLACE VIEW values_often AS
        WITH often AS (
            SELECT column_id
            FROM column_values
            WHERE value != 'example text'
            GROUP BY column_id
            HAVING COUNT(DISTINCT value) > 5 OR COUNT(value) > 50
        )
        SELECT column_values.column_id, column_values.value
        FROM column_values
        WHERE column_values.column_id IN (SELECT column_id FROM often);
    """
    con.execute(view_query)
    # get the number of distinct columns in the view

    count = con.execute("SELECT COUNT(DISTINCT column_id) FROM values_often").fetchone()[0]
    print(f"Number of distinct columns in the view: {count}")

    # create a view with stats on the values
    stats_view = """
                 CREATE OR REPLACE VIEW value_stats AS
                 WITH usages AS (
                    SELECT unnest(column_ids) as column_id, usage_type
                    FROM column_usages
                 )
                 SELECT values_often.column_id, MIN(column_name) AS column_name, MIN(table_name) AS table_name,
                        COUNT(DISTINCT value) AS distinct_value_count,
                        COUNT(value)          AS total_value_count,
                        AVG(LENGTH(value))    AS avg_value_length,
                        MIN(LENGTH(value))    AS min_value_length,
                        MAX(LENGTH(value))    AS max_value_length,
                        max_value_length - MIN(LENGTH(value)) AS value_length_range,
                        COUNT(DISTINCT value) * 1.0 / COUNT(value) AS distinct_ratio,
                        AVG(CASE WHEN value ~ '^[a-zA-Z]+$' THEN 1 ELSE 0 END) AS alpha_ratio,
                        AVG(CASE WHEN value ~ '[^a-zA-Z0-9]' THEN 1 ELSE 0 END) AS special_char_ratio,
                        AVG(CASE WHEN value ~ '^[0-9]+$' THEN 1 ELSE 0 END) AS numeric_ratio,
                        normalized_entropy(string_agg(value)) AS normalized_entropy,
                        list_distinct(list(usage_type)) AS usage_types,
                        list(value)[0:10] AS sample_values
                     
                 FROM values_often
                 JOIN usages ON values_often.column_id = usages.column_id
                 JOIN columns ON values_often.column_id = columns.id
                 JOIN tables ON columns.table_id = tables.id
                 GROUP BY values_often.column_id;
                 """
    con.execute(stats_view)
    # get the stats
    stats = con.execute("SELECT * FROM value_stats").fetchall()
    for row in stats:
        print(row)

    # print number of stats
    count = con.execute("SELECT COUNT(*) FROM value_stats").fetchone()[0]
    print(f"Number of stats: {count}")



if __name__ == "__main__":
    analysis_2()

