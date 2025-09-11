from typing import List, Literal

from src.sql_analysis.tools.sql_types import BaseType


ExecutionMode = Literal['replace', 'append', 'restart']
# replace: an repo that will be executed will replace any existing data for that repo
# append: a repo that was executed will not be executed again, but new repos will be added
# restart: all existing data will be deleted and the repo will be executed again


class Column:
    def __init__(self, column_id: int, column_name: str, column_base_type: BaseType):
        self.column_id: int = column_id
        self.column_name: str = column_name
        self.column_base_type: BaseType = column_base_type

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
