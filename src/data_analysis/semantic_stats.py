from typing import Optional

from src.config import DATABASE_PATH

import duckdb
import pandas as pd
import re
from typing import List, Tuple, Optional

SEMANTIC_RULES: List[Tuple[str, List[str]]] = [
    ("Name",       [r"\bname\b", r"\btitle\b", r"\busername\b", r"\bfirst_name\b", r"\blast_name\b", r"\bfull_name\b",
                    r"\bdisplay_name\b", r"\bnome\b", r"\nombre\b", r"\bprenom\b", r"\bnome_completo\b", r"\blogin\b",
                    r"\bnombre_completo\b", r"\bauthor\b"]),
    ("DateTime",   [r"\bdate\b", r"\btime\b", r"timestamp", r"_at$", r"_on$"]),
    ("Amount",     [r"\bamount\b", r"\bprice\b", r"\btotal\b", r"\bcost\b", r"\bquantity\b"]),
    ("Email",      [r"email", r"\bmail\b"]),
    ("IPAddress", [r"\bip\b", r"ip_address", r"ipv4", r"ipv6"]),
    ("Password",   [r"\bpassword\b", r"pass", r"secret", r"key", r"token"]),
    ("PhoneNumber",[r"phone", r"\btel\b", r"mobile", r"contact", r"number$"]),
    ("URL",        [r"url", r"link", r"\buri\b", r"\bweb\b", r"path", r"website", r"homepage", r"domain", r"\bfile\b", r"image", r"slug"]),
    ("Location",    [r"address", r"location", r"addr", "city", "state", "country", "zip", "postal_code"]),
    ("Category",   [r"\bcategory\b", r"\btype\b", r"\bclass\b", r"sex", r"role", r"genre", r"gender", r"group", r"label", r"language", r"\bstatus\b", r"\bstate\b", r"\bcondition\b"]),
    ("Hash",        [r"\bhash\b", r"\bchecksum\b", r"\bmd5\b", r"\bsha256\b", r"salt", r"signature", r"fingerprint", r"token"]),
    ("Description",[r"description", r"details", r"info", r"descripcio", r"summary", r"content", r"comment", r"message", "remark", "text", "note", "feedback", "subject"]),
    ("DateTime", [r"date", r"time", r"timestamp", r"_at", r"_on"]),
    ("ID", [r"uuid", r"id", r"key", r"code", r"version"]),
    ("Boolean", [r"^is", r"^are", r"^has", r"^have", r"yes_no", r"enable"]),
    ("Name", [r"name", r"creator"]),
    ("Test", [r"column", r"test", r"col"]),  # Test column names that are not semantic
    ("DateTime", [r"date", r"time", r"day"]),
    ("Category", [r"category", r"type", r"class", r"status", r"state", r"condition", r"role"]),
]



def column_name_to_semantic_type_syntactic(column_name: str) -> Optional[str]:
    column_name = column_name.lower()

    for semantic_type, patterns in SEMANTIC_RULES:
        for pattern in patterns:
            if re.search(pattern, column_name):
                return semantic_type

    return None

print(column_name_to_semantic_type_syntactic("example_name"))  # Example usage, should return "Name"

def main():

    con = duckdb.connect(DATABASE_PATH)
    con.create_function(
        "column_name_to_semantic_type_syntactic",
        column_name_to_semantic_type_syntactic,
        null_handling='SPECIAL',
    )

    # get all column usages
    column_usages_df = con.execute("""

        SELECT 
            lower(column_name), 
            FIRST(column_name_to_semantic_type_syntactic(column_name)) as sem_type, COUNT(*) as cnt, 
            COUNT(DISTINCT table_id) as table_cnt, COUNT(DISTINCT repo_id) as repo_cnt
        FROM columns 
        JOIN tables ON columns.table_id = tables.id
        WHERE column_base_type = 'Text' AND column_name_to_semantic_type_syntactic(column_name) IS NULL
        GROUP BY ALL
        ORDER BY repo_cnt DESC
        LIMIT 100;
        -- Set the semantic_type_syntactic column for columns with no semantic type syntactic
            
    """).df()
    print("Columns with no semantic type syntactic:")
    # print the whole dataframe as a md table
    print(column_usages_df.to_markdown(index=False))

    con.execute("""
        ALTER TABLE columns
        ADD COLUMN IF NOT EXISTS semantic_type_syntactic VARCHAR;
    """)
    # Update the semantic_type_syntactic column for columns with no semantic type syntactic
    con.execute("""
        UPDATE columns
        SET semantic_type_syntactic = column_name_to_semantic_type_syntactic(column_name)
    """)


if __name__ == "__main__":
    main()