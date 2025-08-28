import re
from typing import Tuple, Literal

_INT_CANONICALS = (
    (r'\b(bit|bitsigned|bitu|bitunsigned|int1|int1signed|int1u|int1unsigned|int8|int8signed|int8u|int8unsigned|signedbit|signedint1|signedint8|signedtinyint|signedtinyinteger|signedtinyserial|tinyint|tinyinteger|tinyintegersigned|tinyintegeru|tinyintegerunsigned|tinyintsigned|tinyintu|tinyintunsigned|tinyserial|tinyserialsigned|tinyserialu|tinyserialunsigned|ubit|uint1|uint8|unsignedbit|unsignedint1|unsignedint8|unsignedtinyint|unsignedtinyinteger|unsignedtinyserial|utinyint|utinyinteger|utinyserial)\b', 'Int8', 'UInt8'),
    (r'\b(int16|int16signed|int16u|int16unsigned|int2|int2signed|int2u|int2unsigned|signedint16|signedint2|signedsmallint|signedsmallinterger|signedsmallserial|smallint|smallinterger|smallintergersigned|smallintergeru|smallintergerunsigned|smallintsigned|smallintu|smallintunsigned|smallserial|smallserialsigned|smallserialu|smallserialunsigned|uint16|uint2|unsignedint16|unsignedint2|unsignedsmallint|unsignedsmallinterger|unsignedsmallserial|usmallint|usmallinterger|usmallserial)\b', 'Int16', 'UInt16'),
    (r'\b(int24|int24signed|int24u|int24unsigned|int3|int3signed|int3u|int3unsigned|mediumint|mediuminterger|mediumintergersigned|mediumintergeru|mediumintergerunsigned|mediumintsigned|mediumintu|mediumintunsigned|signedint24|signedint3|signedmediumint|signedmediuminterger|uint24|uint3|umediumint|umediuminterger|unsignedint24|unsignedint3|unsignedmediumint|unsignedmediuminterger)\b', 'Int24', 'UInt24'),
    (r'\b(int|int32|int32signed|int32u|int32unsigned|int4|int4signed|int4u|int4unsigned|integer|integersigned|integeru|integerunsigned|intsigned|intu|intunsigned|serial|serialsigned|serialu|serialunsigned|signedint|signedint32|signedint4|signedinteger|signedserial|uint|uint32|uint4|uinteger|unsignedint|unsignedint32|unsignedint4|unsignedinteger|unsignedserial|userial)\b', 'Int32', 'UInt32'),
    (r'\b(bigint|biginteger|bigintegersigned|bigintegeru|bigintegerunsigned|bigintsigned|bigintu|bigintunsigned|bigserial|bigserialsigned|bigserialu|bigserialunsigned|int64|int64signed|int64u|int64unsigned|int8|int8signed|int8u|int8unsigned|long|longsigned|longu|longunsigned|signedbigint|signedbiginteger|signedbigserial|signedint64|signedint8|signedlong|ubigint|ubiginteger|ubigserial|uint64|uint8|ulong|unsignedbigint|unsignedbiginteger|unsignedbigserial|unsignedint64|unsignedint8|unsignedlong)\b', 'Int64', 'UInt64'),
    (r'\b(bigint128|bigint128signed|bigint128u|bigint128unsigned|hugeint|hugeinteger|hugeintegersigned|hugeintegeru|hugeintegerunsigned|hugeintsigned|hugeintu|hugeintunsigned|int128|int128signed|int128u|int128unsigned|signedbigint128|signedhugeint|signedhugeinteger|signedint128|ubigint128|uhugeint|uhugeinteger|uint128|unsignedbigint128|unsignedhugeint|unsignedhugeinteger|unsignedint128)\b', 'Int128', 'UInt128'),
)

_FLOAT_CANONICALS = (
    (r'\b(binary16|binary16signed|binary16u|binary16unsigned|float16|float16signed|float16u|float16unsigned|float2|float2signed|float2u|float2unsigned|fp16|fp16signed|fp16u|fp16unsigned|half|half_float|half_floatsigned|half_floatu|half_floatunsigned|halffloat|halffloatsigned|halffloatu|halffloatunsigned|halfprecision|halfprecisionsigned|halfprecisionu|halfprecisionunsigned|halfsigned|halfu|halfunsigned|signedbinary16|signedfloat16|signedfloat2|signedfp16|signedhalf|signedhalf_float|signedhalffloat|signedhalfprecision|ubinary16|ufloat16|ufloat2|ufp16|uhalf|uhalf_float|uhalffloat|uhalfprecision|unsignedbinary16|unsignedfloat16|unsignedfloat2|unsignedfp16|unsignedhalf|unsignedhalf_float|unsignedhalffloat|unsignedhalfprecision)\b', 'Float16', 'UFloat16'),
    (r'\b(binary32|binary32signed|binary32u|binary32unsigned|float|float32|float32signed|float32u|float32unsigned|float4|float4signed|float4u|float4unsigned|floatsigned|floatu|floatunsigned|fp32|fp32signed|fp32u|fp32unsigned|real|realsigned|realu|realunsigned|signedbinary32|signedfloat|signedfloat32|signedfloat4|signedfp32|signedreal|signedsingle|signedsingleprecision|single|singleprecision|singleprecisionsigned|singleprecisionu|singleprecisionunsigned|singlesigned|singleu|singleunsigned|ubinary32|ufloat|ufloat32|ufloat4|ufp32|unsignedbinary32|unsignedfloat|unsignedfloat32|unsignedfloat4|unsignedfp32|unsignedreal|unsignedsingle|unsignedsingleprecision|ureal|usingle|usingleprecision)\b', 'Float32', 'UFloat32'),
    (r'\b(binary64|binary64signed|binary64u|binary64unsigned|double|doubleprecision|doubleprecisionsigned|doubleprecisionu|doubleprecisionunsigned|doublesigned|doubleu|doubleunsigned|float64|float64signed|float64u|float64unsigned|float8|float8signed|float8u|float8unsigned|fp64|fp64signed|fp64u|fp64unsigned|signedbinary64|signeddouble|signeddoubleprecision|signedfloat64|signedfloat8|signedfp64|ubinary64|udouble|udoubleprecision|ufloat64|ufloat8|ufp64|unsignedbinary64|unsigneddouble|unsigneddoubleprecision|unsignedfloat64|unsignedfloat8|unsignedfp64)\b', 'Float64', 'UFloat64'),
    (r'\b(binary80|binary80signed|binary80u|binary80unsigned|extendedprecision|extendedprecisionsigned|extendedprecisionu|extendedprecisionunsigned|float80|float80signed|float80u|float80unsigned|longdouble|longdoublesigned|longdoubleu|longdoubleunsigned|signedbinary80|signedextendedprecision|signedfloat80|signedlongdouble|ubinary80|uextendedprecision|ufloat80|ulongdouble|unsignedbinary80|unsignedextendedprecision|unsignedfloat80|unsignedlongdouble)\b', 'Float80', 'UFloat80'),
    (r'\b(binary128|binary128signed|binary128u|binary128unsigned|float128|float128signed|float128u|float128unsigned|fp128|fp128signed|fp128u|fp128unsigned|quad|quadruple|quadruplesigned|quadrupleu|quadrupleunsigned|quadsigned|quadu|quadunsigned|signedbinary128|signedfloat128|signedfp128|signedquad|signedquadruple|ubinary128|ufloat128|ufp128|unsignedbinary128|unsignedfloat128|unsignedfp128|unsignedquad|unsignedquadruple|uquad|uquadruple)\b', 'Float128', 'UFloat128'),
)

_TEXT_VARYING  = re.compile(r'\b(char[ _]?varying|string|longvarchar|varchar|character|charactervarying|longtext|nvarchar|varchar2|nvarchar2|text|clob)\b')
_TEXT_FIXED = re.compile(r'\b(char|nchar|bpchar|mediumtext|tinytext|character varying)\b')
_BOOLEAN       = re.compile(r'\b(bool|boolean|boolean_char)\b')
_DATE_TIME     = re.compile(r'\b(date|time|datetime|datetime2|time_stamp|timestamp|timestamptz|smalldatetime|timetz|interval)\b')
_BINARY        = re.compile(r'\b(blob|binary|varbinary|bytea|image|longblob|mediumblob|tinyblob)\b')
_JSON_TYPE     = re.compile(r'\b(json|jsonb)\b')
_UUID_TYPE     = re.compile(r'\b(uuid|uniqueidentifier)\b')
_XML_TYPE      = re.compile(r'\b(xml)\b')
_ENUM_TYPE     = re.compile(r'\b(enum|set)\b')

BaseType = Literal[ "Int", "Float", "Text", "Boolean", "DateTime", "Binary", "JSON", "UUID", "XML", "Enum", "ARRAY", "OTHER" ]

def base_type_to_duckdb_type(base_type: BaseType) -> str:
    """
    Converts a base type to a DuckDB type string.
    """
    if base_type == "Int":
        return "INTEGER"
    elif base_type == "Float":
        return "DOUBLE"
    elif base_type == "Text":
        return "VARCHAR"
    elif base_type == "Boolean":
        return "BOOLEAN"
    elif base_type == "DateTime":
        return "TIMESTAMP"
    elif base_type == "Binary":
        return "BLOB"
    elif base_type == "JSON":
        return "JSON"
    elif base_type == "UUID":
        return "UUID"
    elif base_type == "XML":
        return "XML"
    elif base_type == "Enum":
        return "VARCHAR"  # Enums are often stored as strings
    elif base_type == "ARRAY":
        return "ARRAY"  # DuckDB supports array types
    else:
        return "OTHER"  # Fallback for unrecognized types

def base_type_to_example_value(base_type: BaseType) -> str:
    """
    Returns an example value for a given base type.
    """
    if base_type == "Int":
        return "42"
    elif base_type == "Float":
        return "3.14"
    elif base_type == "Text":
        return "'example text'"
    elif base_type == "Boolean":
        return "TRUE"
    elif base_type == "DateTime":
        return "'2023-10-01 12:00:00'"
    elif base_type == "Binary":
        return "'\\xDEADBEEF'"  # Example binary data
    elif base_type == "JSON":
        return "'{\"key\": \"value\"}'"
    elif base_type == "UUID":
        return "'123e4567-e89b-12d3-a456-426614174000'"
    elif base_type == "XML":
        return "<root><element>value</element></root>"
    elif base_type == "Enum":
        return "'enum_value'"
    elif base_type == "ARRAY":
        return "[1, 2, 3]"
    else:
        return "NULL"  # Fallback for unrecognized types

def unify_type(raw_type: str) -> Tuple[str, BaseType]:
    """
    returns the canonical type name and a basic type category
    """
    if raw_type is None:
        return "OTHER", "OTHER"

    if raw_type.lower().strip() == "array":
        return "ARRAY", "ARRAY"

    t = raw_type.strip().lower()

    # handle "number" type (see https://www.ibm.com/docs/en/db2-warehouse?topic=compatability-number)
    if t == 'number' or t == 'numeric':
        return t, 'Int'

    # if there is a precistion scale, try to cast it
    if 'number' in t or 'decimal' in t:
        match = re.search(r'\((\d+)(?:,(\d+))?\)', t)
        if match:
            precision = match.group(1)
            scale = match.group(2) if match.group(2) else '0'
            t = f"number({precision},{scale})"

            if scale == '0':
                return t, "Int"
            else:
                return t, "Float"


    # remove any size/precision qualifiers

    t = re.sub(r'\(.*\)', '', t)  # drop size/precision qualifiers
    unsigned = "unsigned" in t or t[0] == 'u' # basic unsigned detection

    # --- integer families ---
    for pattern, signed_name, unsigned_name in _INT_CANONICALS:
        if re.search(pattern, t):
            return unsigned_name if unsigned else signed_name, "Int"

    # --- floating point / fixed-point numbers ---
    for pattern, signed_name, unsigned_name in _FLOAT_CANONICALS:
        if re.search(pattern, t):
            return unsigned_name if unsigned else signed_name, "Float"

    # --- enum / set types ---
    if _ENUM_TYPE.search(t):
        return "Enum", "Enum"

    # --- text families ---
    if _TEXT_VARYING.search(t):
        return "VARCHAR", "Text"
    if _TEXT_FIXED.search(t):
        return "CHAR", "Text"

    # --- boolean types ---
    if _BOOLEAN.search(t):
        return "Boolean", "Boolean"

    # --- date and time types ---
    if _DATE_TIME.search(t):
        return "Timestamp", "DateTime"

    # --- binary / blob types ---
    if _BINARY.search(t):
        return "Binary", "Binary"

    # --- JSON ---
    if _JSON_TYPE.search(t):
        return "JSON", "JSON"

    # --- UUID / GUID ---
    if _UUID_TYPE.search(t):
        return "UUID", "Int"

    # --- XML ---
    if _XML_TYPE.search(t):
        return "XML", "XML"

    # --- fallback ---
    return "OTHER", "OTHER"


def test_decimal():
    type = 'DECIMAL(7,2)'
    canonical, base_type = unify_type(type)
