from src.config import get_con


# for all columns that have there column_type = char, set column_base_type to Int instead of Text

def fix_column_base_type(con):
    con.execute("""
        UPDATE columns
        SET column_base_type = 'Int'
        WHERE lower(column_type) = 'char'
    """)


def main():
    con = get_con()
    fix_column_base_type(con)
    con.commit()