import duckdb

from src.config import DATABASE_PATH


def get_joins():


    con = duckdb.connect(DATABASE_PATH, read_only=True)
    result = con.execute(f"""
                SELECT sql 
                FROM queries 
                JOIN tables ON tables.repo_id = queries.repo_id
                JOIN columns ON columns.table_id = tables.id
                WHERE 
                    contains(lower(sql), 'join') 
                    AND type = 'SELECT'
                    AND contains(lower(sql), tables.table_name_clean)
                    AND contains(lower(sql), columns.column_name)
            """).fetchall()

    print(f"Found {len(result)} queries with joins that are not in the column usages table.")


if __name__ == "__main__":
    get_joins()