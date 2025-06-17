from typing import List


class Column:
    def __init__(self, column_id: int, column_name: str, column_base_type: str):
        self.column_id = column_id
        self.column_name = column_name
        self.column_base_type = column_base_type

    def __repr__(self):
        return f"Column(id={self.column_id}, name='{self.column_name}', base_type='{self.column_base_type}')"

class Table:
    def __init__(self, table_id: int, table_name: str, columns: List[Column]):
        self.table_id = table_id
        self.table_name = table_name
        self.columns = columns

    def __repr__(self):
        string = f"Table(id={self.table_id}, name='{self.table_name}', columns=["
        string += ', '.join([repr(column) for column in self.columns])
        string += '])'
        return string