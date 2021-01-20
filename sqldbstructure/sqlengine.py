from abc import ABC, abstractmethod
import pyodbc
import sqldbstructure.sqldatabase as sqldatabase
import sqldbstructure.structureexcept as ex
import warnings

class SQLEngine(ABC):

    def __init__(self, uid, pwd, server, driver, database=None, **kwargs):
        self.uid = uid
        self.pwd = pwd
        self.server = server
        self.driver = driver
        self.database = database
        for k, v in kwargs.items():
            self.__setattr__(k, v)
        self.connection = None

    def _execute_sql_information_schema_database(self, statement):
        """
        """
        if self.connection is None:
            raise ex.SQLConnectionError("Connection is empty - connect to Engine")

        cursor = self.connection.cursor()
        cursor.execute(statement)
        rows = cursor.fetchone()
        cursor.close()
        return rows

    def _execute_sql_information_schema_table(self, statement):
        """
        """
        if self.connection is None:
            raise ex.SQLConnectionError("Connection is empty - connect to Engine")

        cursor = self.connection.cursor()
        cursor.execute(statement)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def _execute_sql_information_schema_columns(self, statement):
        """
        """
        return self._execute_sql_information_schema_table(statement)

    def _execute_sql_transfer_table(self, start_connection, create, select, insert):
        """
        """
        if self.connection is None or start_connection is None:
            raise ex.SQLConnectionError("Connection is empty - connect to Engine")

        start_cursor = start_connection.cursor()
        end_cursor = self.connection.cursor()
        end_cursor.execute(create)
        start_cursor.execute(select)
        row = start_cursor.fetchone()

        while row:
            end_cursor.execute(insert, *row)
            row = start_cursor.fetchone()

    def _build_database(self, database):
        """
        """
        if database is None:
            self.database = None
        else:
            self.sql_activate_database(database=database)

    def _build_connection_dict(self):
        connection_dict = {k: f"{str(v)}" for k, v in self.__dict__.items() if v is not None}
        return connection_dict

    @abstractmethod
    def sql_activate_database(self, database):
        pass

    @abstractmethod
    def _sql_information_schema_database(self, database):
        pass

    @abstractmethod
    def _sql_information_schema_table(self, database):
        pass

    @abstractmethod
    def _sql_information_schema_column(self, database, table):
        pass

    @abstractmethod
    def __repr__(self):
        pass

class SQLServerEngine(SQLEngine):

    def __init__(self, uid, pwd, server, driver, database=None, **kwargs):
        super().__init__(uid, pwd, server, driver, database, **kwargs)
        self.connection = pyodbc.connect(**self._build_connection_dict(), autocommit=True)
        self._build_database(database=database)

    def sql_activate_database(self, database):
        """
        """
        database_statement = self._sql_information_schema_database(database)
        database_exists = self._execute_sql_information_schema_database(database_statement)
        if database_exists[0] == 0:
            raise ex.SQLDatabaseError("Database does not exist")

        self.database = sqldatabase.SQLServerDatabase(database)
        table_statement = self._sql_information_schema_table(database)
        tables = self._execute_sql_information_schema_table(table_statement)

        for table in tables:
            column_statement = self._sql_information_schema_column(database, table)
            columns = self._execute_sql_information_schema_columns(column_statement)
            schema, table = table[0], table[1]
            self.database.construct_database(schema, table, columns)

    def sql_transfer_tables_from_mysql(self, sqlengine, tables):
        """
        """
        if not isinstance(sqlengine, MySQLEngine):
            raise ex.SQLEngineError("Input SQL Server Engine")

        if not isinstance(tables, list):
            raise ex.SQLEngineError("Input tables in a list")

        sqlengine.database.construct_transfer_database(tables)
        self.database.transfer_tables = sqlengine.database.convert_transfer_database(str(self))

        if self.database.transfer_tables is None:
            raise ex.SQLTransferError('No tables to transfer')

        for k, v in self.database.transfer_tables.items():
            create = v.sql_create_table(self.database.database)
            insert = v.sql_insert_table(self.database.database)
            select = sqlengine.database.tables.get(k).sql_select_table(sqlengine.database.database)

            try:
                self._execute_sql_transfer_table(sqlengine.connection, create, select, insert)
            except pyodbc.Error:
                warn_string = f"Unable to transfer {k} into {self.database.database}"
                warnings.warn(warn_string)
                continue

        self.database.transfer_tables = {}
        sqlengine.database.transfer_tables = {}
        database = self.database.database
        self.sql_activate_database(database=database)  # create a method that adds the new table to the self.tables

    def sql_create_custom_table(self, query_statement, database, schema, table, drop_table=True):
        """
        """
        if self.connection is None:
            raise ex.SQLConnectionError("Connection is empty - connect to Engine")

        cursor = self.connection.cursor()

        database_statement = self._sql_information_schema_database(database)
        database_exists = self._execute_sql_information_schema_database(database_statement)
        if database_exists[0] == 0:
            raise ex.SQLDatabaseError("Database does not exist")

        if drop_table:
            cursor.execute(f"drop table if exists {database}.{schema}.{table};")

        full_statement = f"select * into {database}.{schema}.{table} from ({query_statement}) as t1;"
        cursor.execute(full_statement)

        if database.lower() == self.database.database.lower():
            column_statement = self._sql_information_schema_column(database, table)
            columns = self._execute_sql_information_schema_columns(column_statement)
            self.database.construct_database(schema, table, columns)
        cursor.close()

    def _sql_information_schema_database(self, database):
        """
        """
        statement = f"select count(*) as DBExists from sys.databases where name = '{database}'"
        return statement

    def _sql_information_schema_table(self, database):
        """
        """
        statement = f"""
             select table_schema, table_name
             from {database}.information_schema.tables
             where table_type = 'BASE TABLE'
         """
        return statement

    def _sql_information_schema_column(self, database, table):
        """
        """
        schema, table = table[0], table[1]
        statement = f"""
            select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, datetime_precision, is_nullable, character_set_name
            from {database}.information_schema.columns
            where table_schema = 'dbo' and table_name = '{table}'
            order by ordinal_position
            """
        return statement

    def __repr__(self):
        return f"{self.__class__.__name__}()"

class MySQLEngine(SQLEngine):

    def __init__(self, uid, pwd, server, driver, database=None, **kwargs):
        super().__init__(uid, pwd, server, driver, database, **kwargs)
        # self.connection = mysql.connector.connect(**self._build_connection_dict(), autocommit=True)
        self.connection = pyodbc.connect(**self._build_connection_dict(), autocommit=True)
        self._build_database(database=database)

    def sql_activate_database(self, database):
        """
        """
        database_statement = self._sql_information_schema_database(database)
        database_exists = self._execute_sql_information_schema_database(database_statement)
        if database_exists[0] == 0:
            raise ex.SQLDatabaseError("Database does not exist")

        self.database = sqldatabase.MySQLDatabase(database=database)
        table_statement = self._sql_information_schema_table(database)
        tables = self._execute_sql_information_schema_table(table_statement)

        for table in tables:
            column_statement = self._sql_information_schema_column(database, table)
            columns = self._execute_sql_information_schema_columns(column_statement)
            table = table[1]
            self.database.construct_database(table=table, columns=columns)

    def sql_transfer_tables_from_sqlserver(self, sqlengine, tables):
        """
        """
        if not isinstance(sqlengine, SQLServerEngine):
            raise ex.SQLEngineError("Input SQL Server Engine")

        if not isinstance(tables, list):
            raise ex.SQLEngineError("Input tables in a list")

        sqlengine.database.construct_transfer_database(tables)
        self.database.transfer_tables = sqlengine.database.convert_transfer_database(str(self))

        if self.database.transfer_tables is None:
            raise ex.SQLTransferError('No tables to transfer')

        for k, v in self.database.transfer_tables.items():
            create = v.sql_create_table(self.database.database)
            insert = v.sql_insert_table(self.database.database)
            select = sqlengine.database.tables.get(k).sql_select_table(sqlengine.database.database)
            self._execute_sql_transfer_table(sqlengine.connection, create, select, insert)

        self.database.transfer_tables = {}
        sqlengine.database.transfer_tables = {}
        database = self.database.database
        self.sql_activate_database(database=database)

    def sql_create_custom_table(self, query_statement, database, table, drop_table=True):
        """
        """
        if self.connection is None:
            raise ex.SQLConnectionError("Connection is empty - connect to Engine")

        cursor = self.connection.cursor()

        database_statement = self._sql_information_schema_database(database)
        database_exists = self._execute_sql_information_schema_database(database_statement)
        if database_exists[0] == 0:
            raise ex.SQLDatabaseError("Database does not exist")

        if drop_table:
            cursor.execute(f"drop table if exists {database}.{table};")

        full_statement = f"create table {database}.{table} as {query_statement};"
        cursor.execute(full_statement)

        if database.lower() == self.database.database.lower():
            column_statement = self._sql_information_schema_column(database, table)
            columns = self._execute_sql_information_schema_columns(column_statement)
            self.database.construct_database(table, columns)
        cursor.close()

    def _sql_information_schema_database(self, database):
        """
        """
        statement = f"select count(*) as DBExists from information_schema.schemata where schema_name = '{database}'"
        return statement

    def _sql_information_schema_table(self, database):
        """
        """
        statement = f"""
            select table_schema, table_name
            from information_schema.tables
            where table_schema = '{database}' and table_type = 'BASE TABLE'
        """
        return statement

    def _sql_information_schema_column(self, database, table):
        """
        """
        table = table[1]
        statement = f"""
            select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, datetime_precision, is_nullable, character_set_name 
            from information_schema.columns
            where table_schema = '{database}' and table_name = '{table}'
            order by ordinal_position
        """
        return statement

    def __repr__(self):
        return f"{self.__class__.__name__}()"