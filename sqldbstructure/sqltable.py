from abc import ABC, abstractmethod
import sqldbstructure.sqlcolumn as sqlcolumn

class SQLTable(ABC):

    def __init__(self, table):
        self.table = table
        self.columns = None

    def _construct_table(self, columns):
        """
        """
        table_columns = []
        for column in columns:
            column_object = getattr(sqlcolumn, self.__class__.__name__.replace('Table', 'Column'))(*column)
            table_columns.append(column_object)
        return table_columns

    def convert_table(self, software):
        """
        """
        converted_columns = []
        for column in self.columns:
            converted_columns.append([v for v in column.convert_column(software).__dict__.values()])
        table_object = globals()[f"{software.replace('Engine()', 'Table')}"](self.table, converted_columns)
        return table_object

    def sql_insert_table(self, database):
        """
        """
        statement = f"insert into {self.format_table(database)} values ("
        statement = statement + ', '.join(['?' for _ in self.columns])
        return statement + ');'

    def sql_create_table(self, database):
        """
        """
        statement = f"create table {self.format_table(database)} ("
        statement = statement + ', '.join([c.format_column() for c in self.columns])
        return statement + ');'

    def sql_select_table(self, database):
        """
        """
        statement = f"select * from {self.format_table(database)};"
        return statement

    @abstractmethod
    def format_table(self, database):
        pass

    @abstractmethod
    def __repr__(self):
        return f"{self.__class__.__name__}({self.table})"

class SQLServerTable(SQLTable):

    def __init__(self, table, columns, schema='dbo'):
        super().__init__(table)
        self.schema = schema
        self.columns = self._construct_table(columns)

    def format_table(self, database):
        return f"{database}.{self.schema}.{self.table}"

    def __repr__(self):
        return f"{self.__class__.__name__}({self.table})"

class MySQLTable(SQLTable):

    def __init__(self, table, columns):
        super().__init__(table)
        self.columns = self._construct_table(columns)

    def format_table(self, database):
        return f"{database}.{self.table}"

    def __repr__(self):
        return f"{self.__class__.__name__}({self.table})"