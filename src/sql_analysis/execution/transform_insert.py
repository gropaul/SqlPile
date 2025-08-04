from typing import Optional


import re
import textwrap
from typing import Optional, List
import duckdb
from src.config import get_con
from src.sql_analysis.execution.models import Table, Column
from src.sql_analysis.execution.prepare_sql_for_execution import escape_for_insert
from src.sql_analysis.load_schemapile_json_to_ddb import COLUMNS_TABLE_NAME, TABLES_TABLE_NAME
from src.sql_analysis.tools.semantic_type import get_column_semantic_type
from src.sql_analysis.tools.sql_types import unify_type


def _split_value_tuples(values_sql: str) -> List[str]:
    """
    Return a list of top-level `( … )` tuples from the VALUES block.

    Handles:
      • nested parentheses (e.g. to_timestamp('…'))
      • single-quoted strings with doubled-quote escaping ('' inside strings)
    """
    tuples: List[str] = []
    depth = 0
    in_string = False
    start = None
    i = 0
    n = len(values_sql)

    while i < n:
        ch = values_sql[i]

        # inside a quoted literal ------------------------------------------------
        if in_string:
            if ch == "'":
                # doubled single quote => escaped quote, stay in string
                if i + 1 < n and values_sql[i + 1] == "'":
                    i += 1  # skip the escape char
                else:
                    in_string = False
        else:
            # outside a quoted literal ------------------------------------------
            if ch == "'":
                in_string = True

            elif ch == "(":
                if depth == 0:
                    start = i
                depth += 1

            elif ch == ")":
                depth -= 1
                if depth == 0 and start is not None:
                    tuples.append(values_sql[start : i + 1].strip())
                    start = None

        i += 1

    return tuples


def transform_insert_to_create(insert_sql: str) -> Optional[str]:
    """
    Transform a simple INSERT-VALUES statement (with an explicit column list)
    into a CREATE TABLE … AS SELECT statement.

    Returns None when the SQL is not transformable.

    Supports value expressions containing nested parentheses such as
    `to_timestamp(...)`.
    """
    if not insert_sql:
        return None

    sql = insert_sql.strip().rstrip(";")

    # 1️⃣  Pull out table name, column list, and *everything* after VALUES -------
    m = re.match(
        r"""
        ^\s*insert\s+into\s+
        (?P<table>[a-zA-Z_][\w$]*)\s*              # table name
        \(\s*(?P<cols>[^)]*?)\s*\)\s*              # (col1, …)
        values\s*(?P<vals>.*)$                     # everything after VALUES
        """,
        sql,
        flags=re.IGNORECASE | re.VERBOSE | re.DOTALL,
    )
    if not m:
        return None

    table = m.group("table").strip()
    columns_raw = m.group("cols").strip()
    values_raw = m.group("vals").strip()
    if not columns_raw:
        return None

    columns = [c.strip() for c in columns_raw.split(",") if c.strip()]
    if not columns:
        return None

    # 2️⃣  Extract the top-level value tuples safely -----------------------------
    value_tuples = _split_value_tuples(values_raw)
    if not value_tuples:
        return None

    # 3️⃣  Assemble CREATE statement -------------------------------------------
    values_sql = ",\n            ".join(value_tuples)
    columns_sql = ", ".join(columns)

    create_sql = textwrap.dedent(
        f"""\
        CREATE TABLE {table} AS
        SELECT *
        FROM (
            VALUES {values_sql}
        ) AS t ({columns_sql});
        """
    )

    return create_sql.lower()



def save_schema_from_insert_create(repo_id: int, sql: str, con: duckdb.DuckDBPyConnection, sandbox_con: duckdb.DuckDBPyConnection) -> Table:
    columns: List[Column] = []

    # find the " AS "
    query_head = sql.lower().split(" as ")[0]
    # remove the "create table " part
    query_head = query_head.replace("create table ", "")
    table_name = query_head.strip().split()[0].strip('`"[]')

    if not table_name:
        raise ValueError("Table name could not be extracted from the SQL statement.")

    max_table_id = con.execute("SELECT MAX(id) FROM tables").fetchone()[0]
    table_id = max_table_id + 1 if max_table_id is not None else 1

    max_column_id = con.execute("SELECT MAX(id) FROM columns").fetchone()[0]
    column_id = max_column_id + 1 if max_column_id is not None else 1

    schema_query = f"SELECT column_name, data_type, ordinal_position FROM information_schema.columns WHERE lower(table_name) = '{table_name}'"
    columns_res = sandbox_con.execute(schema_query).fetchall()

    con.execute(f"""
        INSERT INTO {TABLES_TABLE_NAME} (id, repo_id, table_name, table_name_clean, file_url)
        VALUES (?, ?, ?, ?, ?)
    """, (table_id, repo_id, table_name, table_name.lower()+'_from_insert', None))

    for column_name, data_type, pos in columns_res:
        column_name = column_name.lower()
        column_type, base_type = unify_type(data_type)
        column = Column(
            column_id=column_id,
            column_name=column_name,
            column_base_type=base_type
        )

        semantic_type = get_column_semantic_type(column_name, base_type)
        try:
            con.execute(f"""
                                    INSERT INTO {COLUMNS_TABLE_NAME} (
                                        id, table_id, column_name, column_table_index, column_type, column_base_type,
                                        column_type_original, semantic_type, is_unique, is_nullable,
                                        is_indexed, is_primary_key
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                column_id, table_id, column_name, pos, column_type, base_type,
                column_type, semantic_type, False,
                True, False, False
            ))
        except Exception as e:
            print(f"Error inserting column {column_name} into {COLUMNS_TABLE_NAME}: {e}")
            continue
        columns.append(column)
        column_id += 1
    
    table = Table(
        table_id=table_id,
        table_name=table_name,
        columns=columns
    )


    return table


def test_repo_19391_b():
    insert_sql = """
    insert into c_periodcontrol values ( 'n' , 201394 , 'n' , 'n' , 'mof' , 'adab31e0 - 9590 - 4d6d - 801c - adf1852f3ab2' , 100 ,  100 , 11 , 0 , 'y' , 200045  )
    """

    create_statement = transform_insert_to_create(insert_sql)
    assert create_statement is None




def test_repo_19391():
    insert_sql = """
    insert into c_periodcontrol ( processing , c_periodcontrol_id , periodaction , periodstatus , docbasetype , c_periodcontrol_uu , updatedby , createdby , ad_client_id , ad_org_id , isactive , c_period_id  ) values ( 'n' , 201394 , 'n' , 'n' , 'mof' , 'adab31e0 - 9590 - 4d6d - 801c - adf1852f3ab2' , 100 ,  100 , 11 , 0 , 'y' , 200045  )
    """
    import duckdb

    create_statement = transform_insert_to_create(insert_sql)
    assert create_statement is not None, "The SQL should be transformed into a CREATE TABLE statement."
    # try executing the create statement
    con = duckdb.connect()
    print(f"Executing CREATE TABLE statement:\n{create_statement}")
    con.execute(create_statement)
    try:
        pass
    except Exception as e:
        print(f"Error executing create statement: {e}")
        assert False, "The CREATE TABLE statement should be executable without errors."
    finally:
        # now we should be able to add other inserts to the table
        insert_sql_2 = "insert into c_periodcontrol ( processing , c_periodcontrol_id , periodaction , periodstatus , docbasetype , c_periodcontrol_uu , updatedby , createdby , ad_client_id , ad_org_id , isactive , c_period_id  ) values ( 'n' , 201395 , 'n' , 'n' , 'mcc' , 'ed234746 - 1918 - 4e7b - a93c - 4834be49657a' , 100 , 100 , 11 , 0 , 'y' , 200045 )"
        con.execute(insert_sql_2)
