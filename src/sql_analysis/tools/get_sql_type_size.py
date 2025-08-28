import math
import re
from typing import Optional

from src.config import get_con


def bytes_for_digits(n: int) -> int:
    # log2(10) ≈ 3.32192809489
    bits_needed = math.ceil(n * math.log2(10))
    return math.ceil(bits_needed / 8)


def get_sql_type_size(sql_type: str) -> Optional[int]:
    """
    The types that can come are:
    Text: Return None, as it can be arbitrarily large.
    OTHER, ARRAY, ENUM, Binary, JSON, XML: Return None, as they can be arbitrarily large.
    Integer: Int8, Int16, Int24, Int32, Int64, Int128 + u variants: Return their sizes in bytes.
    Float: Float16, Float32, Float64, Float80, Float128 + U variants: Return their sizes in bytes.
    number(M, D): Calcualte size based on M and D.
    Date/Time: Date, Time, Timestamp, Interval: Return their sizes in bytes.
    Boolean: Return 1 byte.
    UUID: Return 16 bytes.
    """

    sql_type = sql_type.strip().lower()

    if sql_type in {'varchar', 'char'}:
        return None

    if sql_type in {'text', 'other', 'array', 'enum', 'binary', 'json', 'xml'}:
        return None

    # create a mapping for integer types
    int_map = {
        'int8': 1, 'int16': 2, 'int24': 3, 'int32': 4, 'int64': 8, 'int128': 16
    }
    unsigned = {'u' + t: s for t, s in int_map.items()}
    int_map.update(unsigned)

    if sql_type in int_map:
        return int_map[sql_type]

    # create a mapping for float types
    float_map = {
        'float16': 2, 'float32': 4, 'float64': 8, 'float80': 10, 'float128': 16,
        'float': 4, 'double': 8, 'real': 4, 'single': 4, 'doubleprecision': 8,
    }
    ufloat = {'u' + t: s for t, s in float_map.items()}
    float_map.update(ufloat)

    if sql_type in float_map:
        return float_map[sql_type]

    # for number and numeric, the default number of digits is 18
    # https://www.w3schools.com/sql/sql_datatypes.asp#:~:text=numeric-,(,-p%2Cs)
    if sql_type in {'number', 'numeric'}:
        return bytes_for_digits(18)

    # handle decimal types
    decimal_match = re.match(r'number\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', sql_type)
    if decimal_match:
        M = int(decimal_match.group(1))  # total number of digits
        D = int(decimal_match.group(2))  # number of digits after the decimal point
        if M <= 0 or D < 0 or D > M:
            return None
        bytes = bytes_for_digits(M)
        return bytes

    # handle date/time types
    date_time_map = {
        'date': 4,
        'time': 8,
        'timestamp': 8,
        'interval': 16
    }
    if sql_type in date_time_map:
        return date_time_map[sql_type]

    if sql_type == 'boolean':
        return 1

    if sql_type == 'uuid':
        return 16

    print(f"Unknown SQL type: {sql_type}")
    return None


if __name__ == "__main__":
    con = get_con()

    con.execute("""
                CREATE OR REPLACE TABLE sql_type_sizes
                (
                    sql_type      TEXT NOT NULL,
                    size_in_bytes INTEGER
                )
                """)

    types_of_columns = con.execute("""
        SELECT DISTINCT column_type FROM columns
    """).fetchall()

    for (sql_type,) in types_of_columns:
        size = get_sql_type_size(sql_type)
        con.execute("""
            INSERT INTO sql_type_sizes (sql_type, size_in_bytes)
            VALUES (?, ?)
            """, (sql_type, size))
    con.commit()


