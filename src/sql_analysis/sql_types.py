import re
from typing import Tuple

_CANONICALS = (
    # (regex-pattern, signed result, unsigned result)
    (r'\b(tinyint|unsignedtinyint|int1|uint8)\b',                    "Int8",  "UInt8"),
    (r'\b(smallint|unsignedsmallint|int2|smallserial)\b',             "Int16", "UInt16"),
    (r'\b(mediumint|unsignedmediumint|int3|int24)\b',                  "Int24", "UInt24"),  # MySQL MEDIUMINT (24-bit)
    (r'\b(bigint|unsignedbigint|int8|bigserial)\b',                 "Int64", "UInt64"),
    (r'\b(int|unsignedint|unsignedinteger|integer|serial|int4)\b',   "Int32", "UInt32"),  # keep this *after* tiny/small/big rules
)

_TEXT_VARYING  = re.compile(r'\b(char[ _]?varying|string|varchar|charactervarying|longtext|nvarchar|varchar2|nvarchar2|text|clob)\b')
_TEXT_FIXED    = re.compile(r'\b(char|nchar|bpchar|tinytext)\b')
_FLOATING      = re.compile(r'\b(float|float4|float8|double|doubleprecision|double\s+precision|real|decimal|dec|numeric|number)\b')
_BOOLEAN       = re.compile(r'\b(bool|boolean|boolean_char)\b')
_DATE_TIME     = re.compile(r'\b(date|time|datetime|datetime2|time_stamp|timestamp|timestamptz|smalldatetime|timetz|interval)\b')
_BINARY        = re.compile(r'\b(blob|binary|varbinary|bytea|image|longblob|mediumblob|tinyblob)\b')
_JSON_TYPE     = re.compile(r'\b(json|jsonb)\b')
_UUID_TYPE     = re.compile(r'\b(uuid|uniqueidentifier)\b')
_XML_TYPE      = re.compile(r'\b(xml)\b')
_ENUM_TYPE     = re.compile(r'\b(enum|set)\b')

def unify_type(raw_type: str) -> Tuple[str, str]:
    """
    returns the canonical type name and a basic type category
    """
    if raw_type is None:
        return "OTHER", "OTHER"

    t = raw_type.strip().lower()
    t = re.sub(r'\(.*\)', '', t)              # drop size/precision qualifiers
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
