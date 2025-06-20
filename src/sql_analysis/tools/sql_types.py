import re
from typing import Tuple, Literal

prefixes = [
    'signed', 'unsigned', 'u'
]

suffixes = [
    'unsigned', 'signed', 'u'
]
size_types = {
    'Int8':  [ 'int1', 'int8',  'tinyint', 'bit',],
    'Int16': [ 'int2', 'int16', 'smallint','smallserial'],
    'Int24': [ 'int3', 'int24', 'mediumint'],
    'Int32': [ 'int',  'int4',  'int32', 'integer', 'int', 'serial'],
    'Int64': [ 'int8', 'int64', 'bigint', 'bigserial', 'long'],
}

_CANONICALS = (
    (r'\b(bit|bitsigned|bitu|bitunsigned|int1|int1signed|int1u|int1unsigned|int8|int8signed|int8u|int8unsigned|signedbit|signedint1|signedint8|signedtinyint|tinyint|tinyintsigned|tinyintu|tinyintunsigned|ubit|uint1|uint8|unsignedbit|unsignedint1|unsignedint8|unsignedtinyint|utinyint)\b', 'Int8', 'UInt8'),
    (r'\b(int16|int16signed|int16u|int16unsigned|int2|int2signed|int2u|int2unsigned|signedint16|signedint2|signedsmallint|signedsmallserial|smallint|smallintsigned|smallintu|smallintunsigned|smallserial|smallserialsigned|smallserialu|smallserialunsigned|uint16|uint2|unsignedint16|unsignedint2|unsignedsmallint|unsignedsmallserial|usmallint|usmallserial)\b', 'Int16', 'UInt16'),
    (r'\b(int24|int24signed|int24u|int24unsigned|int3|int3signed|int3u|int3unsigned|mediumint|mediumintsigned|mediumintu|mediumintunsigned|signedint24|signedint3|signedmediumint|uint24|uint3|umediumint|unsignedint24|unsignedint3|unsignedmediumint)\b', 'Int24', 'UInt24'),
    (r'\b(int|int32|int32signed|int32u|int32unsigned|int4|int4signed|int4u|int4unsigned|integer|integersigned|integeru|integerunsigned|intsigned|intu|intunsigned|serial|serialsigned|serialu|serialunsigned|signedint|signedint32|signedint4|signedinteger|signedserial|uint|uint32|uint4|uinteger|unsignedint|unsignedint32|unsignedint4|unsignedinteger|unsignedserial|userial)\b', 'Int32', 'UInt32'),
    (r'\b(bigint|bigintsigned|bigintu|bigintunsigned|bigserial|bigserialsigned|bigserialu|bigserialunsigned|int64|int64signed|int64u|int64unsigned|int8|int8signed|int8u|int8unsigned|long|longsigned|longu|longunsigned|signedbigint|signedbigserial|signedint64|signedint8|signedlong|ubigint|ubigserial|uint64|uint8|ulong|unsignedbigint|unsignedbigserial|unsignedint64|unsignedint8|unsignedlong)\b', 'Int64', 'UInt64'),
)


_TEXT_VARYING  = re.compile(r'\b(char[ _]?varying|string|longvarchar|varchar|character|charactervarying|longtext|nvarchar|varchar2|nvarchar2|text|clob)\b')
_TEXT_FIXED = re.compile(r'\b(char|nchar|bpchar|mediumtext|tinytext|character varying)\b')
_FLOATING      = re.compile(r'\b(float|float4|float8|double|doubleprecision|double\s+precision|real|decimal|dec|numeric|number)\b')
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
        return '{"key": "value"}'
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
    if t == 'number' or t == 'numeric' or t == 'decimal':
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
    for pattern, signed_name, unsigned_name in _CANONICALS:
        if re.search(pattern, t):
            return unsigned_name if unsigned else signed_name, "Int"

    # --- floating point / fixed-point numbers ---
    if _FLOATING.search(t):
        return "Float", "Float"

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
