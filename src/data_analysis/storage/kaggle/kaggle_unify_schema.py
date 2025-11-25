from typing import List

from tqdm import tqdm

from src.config import KAGGLE_DATA_DB_PATH
import duckdb


# many kaggle datasets have multiple parquet/csv files that belong to the same table. We need to unify them into a single table.

class ColumnDefinition:
    name: str
    data_type: str
    position: int

    def __init__(self, column_tuple):
        self.name = column_tuple['column_name']
        self.data_type = column_tuple['data_type']
        self.position = column_tuple['position']

    def __repr__(self):
        return f"Column(name='{self.name}', data_type='{self.data_type}', position={self.position})"


class TableDefinition:
    name: str
    columns: List[ColumnDefinition]
    duplicates: List[str]

    def __init__(self, table_tuple):
        self.name = table_tuple['table_name']
        self.columns = []
        for col in table_tuple['columns']:
            self.columns.append(ColumnDefinition(col))

    def __repr__(self):
        string = f"Table(name='{self.name}', columns=["
        string += ', '.join([repr(column) for column in self.columns])
        string += '])'
        return string

    def is_duplicate_of(self, other_table: 'TableDefinition') -> bool:
        if len(self.columns) != len(other_table.columns):
            return False
        # order the columns by position
        self_columns_sorted = sorted(self.columns, key=lambda c: c.position)
        other_columns_sorted = sorted(other_table.columns, key=lambda c: c.position)

        for col1, col2 in zip(self_columns_sorted, other_columns_sorted):
            if col1.data_type != col2.data_type:
                return False

        # the name of all columns must be similar
        # we consider them similar if they share at least half of the characters in the name
        for col1, col2 in zip(self_columns_sorted, other_columns_sorted):
            name1_set = set(col1.name)
            name2_set = set(col2.name)
            common_chars = name1_set.intersection(name2_set)
            if len(common_chars) < min(len(name1_set), len(name2_set)) / 2:
                return False

        return True


class SchemaDefinition:
    name: str
    tables: List[TableDefinition]

    def __init__(self, schema_name: str, tables: List[dict]):
        self.name = schema_name
        self.tables = []
        for table in tables:
            self.tables.append(TableDefinition(table))

    def find_unification(self):
        unified_tables = []
        for table in self.tables:
            found_duplicate = False
            for unified_table in unified_tables:
                if table.is_duplicate_of(unified_table):
                    unified_table.duplicates.append(table.name)
                    break
            if not found_duplicate:
                table.duplicates = []
                unified_tables.append(table)
        self.tables = unified_tables

    def apply_unification(self, con: duckdb.DuckDBPyConnection) -> int:

        n_tables_unified = 0
        for table in self.tables:

            if not table.duplicates:
                continue

            for duplicate_table_name in tqdm(table.duplicates,
                                             desc=f'Unifying tables into {table.name} in schema {self.name}'):
                try:
                    # union the duplicate table into the main table
                    con.execute(
                        f'INSERT INTO "{self.name}"."{table.name}" SELECT * FROM "{self.name}"."{duplicate_table_name}"')

                    # drop the duplicate table from the database
                    con.execute(f'DROP TABLE "{self.name}"."{duplicate_table_name}"')
                    n_tables_unified += 1
                except Exception as e:
                    print(f"Error unifying table {duplicate_table_name} into {table.name} in schema {self.name}: {e}")

        # commit the changes
        con.execute("CHECKPOINT;")
        return n_tables_unified

    def __repr__(self):
        string = f"Schema(name='{self.name}', tables=[\n"
        string += ',\n'.join([repr(table) for table in self.tables])
        string += '\n])'
        return string


def unify_kaggle_table_schema(schema_name: str = None):
    data_con = duckdb.connect(KAGGLE_DATA_DB_PATH)

    where_filter = "" if schema_name is None else f" WHERE table_schema = '{schema_name}' "

    query = """
         SELECT table_schema, list({table_name: table_name, columns: columns}) AS tables 
         FROM (
             SELECT table_schema, table_name, columns: list({column_name: column_name, data_type:data_type, position: ordinal_position}) 
             FROM information_schema.columns 
             GROUP BY table_schema, table_name
             ORDER BY table_schema, table_name
         )
            """ + where_filter + """
         GROUP BY table_schema """

    tables = data_con.execute(query).fetchall()

    n_unified_tables = 0

    with tqdm(total=len(tables), desc="Unifying Kaggle table schemas") as pbar:
        for table_schema, tables_in_schema in tables:
            schema_def = SchemaDefinition(table_schema, tables_in_schema)
            schema_def.find_unification()
            n_unified_tables += schema_def.apply_unification(data_con)

            # update progress bar
            pbar.set_postfix({"n_unified_tables": n_unified_tables})
            pbar.update(1)


if __name__ == "__main__":
    unify_kaggle_table_schema()
