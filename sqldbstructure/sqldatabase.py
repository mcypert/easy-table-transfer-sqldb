from abc import ABC, abstractmethod
import sqldbstructure.sqltable as sqltable
import warnings
from functools import singledispatch
import sys
import sqldbconversion.convertexcept as ex
import copy

class SQLDatabase(ABC):

    def __init__(self, database):
        self.database = database
        self.tables = {}
        self.transfer_tables = {}

    def convert_transfer_database(self, software):
        """
        """
        converted_dict = {}
        for k, v in self.transfer_tables.items():
            try:
                converted_table = v.convert_table(software)
                converted_dict.update({k: converted_table})
            except ex.SQLConversionError:
                warn_string = f"{sys.exc_info()[1]} Table - {v.table} will not be transferred to the database."
                warnings.warn(warn_string)
        return converted_dict

    @abstractmethod
    def __repr__(self):
        return f"{self.__class__.__name__}()"

class SQLServerDatabase(SQLDatabase):

    def __init__(self, database):
        super().__init__(database)

    def construct_database(self, schema, table, columns):
        """
        """
        table_object = sqltable.SQLServerTable(table=table, columns=columns, schema=schema)
        self.tables.update({f"{schema}.{table}": table_object})

    def construct_transfer_database(self, tables):
        """
        """
        for table in tables:
            table_name = _sqlserver_identify_table_name_input(table)
            table_rename = _sqlserver_identify_table_rename_input(table)
            table_object = copy.deepcopy(self.tables.get(table_name))
            if table_object is None:
                warn_string = f"Table: {table} is not in the database"
                warnings.warn(warn_string)
                continue
            self.transfer_tables.update({table_name: table_object})
            self.transfer_tables.get(table_name).table = table_rename

    def __repr__(self):
        return f"{self.__class__.__name__}()"

class MySQLDatabase(SQLDatabase):

    def __init__(self, database):
        super().__init__(database)

    def construct_database(self, table, columns):
        """
        """
        table_object = sqltable.MySQLTable(table=table, columns=columns)
        self.tables.update({f"{table}": table_object})

    def construct_transfer_database(self, tables):
        """
        """
        for table in tables:
            table_name = _mysql_identify_table_name_input(table)
            table_rename = _mysql_identify_table_rename_input(table)
            table_object = copy.deepcopy(self.tables.get(table_name))
            if table_object is None:
                warn_string = f"Table: {table} is not in the database"
                warnings.warn(warn_string)
                continue
            self.transfer_tables.update({table_name: table_object})
            self.transfer_tables.get(table_name).table = table_rename

    def __repr__(self):
        return f"{self.__class__.__name__}()"

####################################################################################################################################################################
####################################################################################################################################################################

@singledispatch
def _sqlserver_identify_table_name_input(table):
    """
    """
    raise NotImplementedError('Unsupported type - input must be either tuple or string')

@_sqlserver_identify_table_name_input.register(tuple)
def _(table):
    """
    """
    return table[0]

@_sqlserver_identify_table_name_input.register(str)
def _(table):
    """
    """
    return table

@singledispatch
def _sqlserver_identify_table_rename_input(table):
    """
    """
    raise NotImplementedError('Unsupported type - input must be either tuple or string')

@_sqlserver_identify_table_rename_input.register(tuple)
def _(table):
    """
    """
    return table[1]

@_sqlserver_identify_table_rename_input.register(str)
def _(table):
    """
    """
    return table.split(".")[1]

####################################################################################################################################################################
####################################################################################################################################################################

@singledispatch
def _mysql_identify_table_name_input(table):
    """
    """
    raise NotImplementedError('Unsupported type - input must be either tuple or string')

@_mysql_identify_table_name_input.register(tuple)
def _(table):
    """
    """
    return table[0]

@_mysql_identify_table_name_input.register(str)
def _(table):
    """
    """
    return table

@singledispatch
def _mysql_identify_table_rename_input(table):
    """
    """
    raise NotImplementedError('Unsupported type - input must be either tuple or string')

@_mysql_identify_table_rename_input.register(tuple)
def _(table):
    """
    """
    return table[1]

@_mysql_identify_table_rename_input.register(str)
def _(table):
    """
    """
    return table

####################################################################################################################################################################
####################################################################################################################################################################