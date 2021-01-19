import unittest
from parameterized import parameterized
from sqldbstructure.sqlcolumn import MySQLColumn, SQLServerColumn
from sqldbstructure.sqltable import MySQLTable, SQLServerTable

sql_server_columns = [
    ('actor_id', 'int', None, 10, 0, None, 'NO', None),
    ('first_name', 'nvarchar', 45, None, None, None, 'NO', 'UNICODE'),
    ('last_name', 'nvarchar', 45, None, None, None, 'NO', 'UNICODE'),
    ('last_update', 'datetime2', None, None, None, 0, 'NO', None)
]

mysql_columns = [
    ('actor_id', 'int', None, 10, 0, None, 'NO', None),
    ('first_name', 'varchar', 45, None, None, None, 'NO', 'UNICODE'),
    ('last_name', 'varchar', 45, None, None, None, 'NO', 'UNICODE'),
    ('last_update',	'datetime', None, None, None, 0, 'NO', None)
]

class SQLTableTest(unittest.TestCase):

    @parameterized.expand([
        ['Test SQL Server Construct Table', 'test_table', 'rpt', sql_server_columns, SQLServerTable]
    ])
    def test_sqlserver_construct_table(self, test_name, table_name, schema, columns, result1):
        """
        """
        table_object = SQLServerTable(table=table_name, schema=schema, columns=columns)
        self.assertEqual(type(table_object), result1, msg=f"{test_name} Table Object Test failed")

    @parameterized.expand([
        ['Test MySQL Construct Table', 'test_table', mysql_columns, MySQLTable]
    ])
    def test_mysql_construct_table(self, test_name, table_name, columns, result1):
        """
        """
        table_object = MySQLTable(table=table_name, columns=columns)
        self.assertEqual(type(table_object), result1, msg=f"{test_name} Table Object Test failed")

    @parameterized.expand([
        ['Test SQL Server convert to MySQL', 'test_table', sql_server_columns, 'SQLServerTable', 'MySQLEngine()', MySQLTable],
        ['Test MySQL convert to SQL Server', 'test_table', mysql_columns, 'MySQLTable', 'SQLServerEngine()', SQLServerTable],
    ])
    def test_convert_table(self, test_name, table_name, columns, start, engine, result):
        """
        """
        table_object = globals()[start](table=table_name, columns=columns)
        converted_table = table_object.convert_table(engine)
        self.assertEqual(type(converted_table), result, msg=f"{test_name} failed")

    @parameterized.expand([
        ['Test SQL Server Create', 'test_table', sql_server_columns, 'Testing', 'SQLServerTable',
         'create table Testing.dbo.test_table (actor_id int not null, first_name nvarchar(45) not null, last_name nvarchar(45) not null, last_update datetime2(0) not null);'
         ],
        ['Test MySQL Create', 'test_table', mysql_columns, 'Testing', 'MySQLTable',
         'create table Testing.test_table (actor_id int not null, first_name varchar(45) not null, last_name varchar(45) not null, last_update datetime(0) not null);'
         ]
    ])
    def test_sql_create_table(self, test_name, table_name, columns, database, start, create_statement_result):
        """
        """
        table_object = globals()[start](table=table_name, columns=columns)
        create_statement = table_object.sql_create_table(database=database)
        self.assertEqual(create_statement, create_statement_result, msg=f"{test_name} failed")

    @parameterized.expand([
        ['Test SQL Server Insert', 'test_table', sql_server_columns, 'Testing', 'SQLServerTable',
         'insert into Testing.dbo.test_table values (?, ?, ?, ?);'
         ],
        ['Test MySQL Insert', 'test_table', mysql_columns, 'Testing', 'MySQLTable',
         'insert into Testing.test_table values (?, ?, ?, ?);'
         ]
    ])
    def test_sql_insert_table(self, test_name, table_name, columns, database, start, insert_statement_result):
        """
        """
        table_object = globals()[start](table=table_name, columns=columns)
        insert_statement = table_object.sql_insert_table(database=database)
        self.assertEqual(insert_statement, insert_statement_result, msg=f"{test_name} failed")

    @parameterized.expand([
        ['Test SQL Server Select', 'test_table', sql_server_columns, 'Testing', 'SQLServerTable',
         'select * from Testing.dbo.test_table;'
         ],
        ['Test MySQL Select', 'test_table', mysql_columns, 'Testing', 'MySQLTable',
         'select * from Testing.test_table;'
         ]
    ])
    def test_sql_insert_table(self, test_name, table_name, columns, database, start, select_statement_result):
        """
        """
        table_object = globals()[start](table=table_name, columns=columns)
        select_statement = table_object.sql_select_table(database=database)
        self.assertEqual(select_statement, select_statement_result, msg=f"{test_name} failed")

    @parameterized.expand([
        ['Test SQL Server Format Table', 'test_table', 'dbo', 'Testing', sql_server_columns, 'Testing.dbo.test_table']
    ])
    def test_sqlserver_format_table(self, test_name, table_name, schema, database, columns, result1):
        """
        """
        table_object = SQLServerTable(table=table_name, schema=schema, columns=columns)
        table_format = table_object.format_table(database=database)
        self.assertEqual(table_format, result1, msg=f"{test_name} Table Object Test failed")

    @parameterized.expand([
        ['Test MySQL Format Table', 'test_table', 'Testing', mysql_columns, 'Testing.test_table']
    ])
    def test_mysql_format_table(self, test_name, table_name, database, columns, result1):
        """
        """
        table_object = MySQLTable(table=table_name, columns=columns)
        table_format = table_object.format_table(database=database)
        self.assertEqual(table_format, result1, msg=f"{test_name} Table Object Test failed")

if __name__ == '__main__':
    unittest.main()