import pyodbc
import unittest
from parameterized import parameterized
import sqldbconnect.sqlpyodbcconnect as sqldb

class MyTestCase(unittest.TestCase):

    database_connections = [
        # SQL Server
        [{'driver': '{ODBC Driver 17 for SQL Server}', 'server': 'DESKTOP-JQ42TS0\SQLEXPRESS', 'database': 'AdventureWorksDW', 'uid': 'mjc', 'pwd': 'peanut'}],
        [{'driver': '{ODBC Driver 17 for SQL Server}', 'server': 'DESKTOP-JQ42TS0\SQLEXPRESS', 'database': 'TSQLV4', 'uid': 'mjc', 'pwd': 'peanut'}],
        [{'driver': '{ODBC Driver 17 for SQL Server}', 'server': 'DESKTOP-JQ42TS0\SQLEXPRESS', 'database': 'Testing', 'uid': 'mjc', 'pwd': 'peanut'}],
        # MySQL
        [{'driver': '{MySQL ODBC 8.0 ANSI Driver}', 'server': 'localhost', 'database': 'adventureworksdw', 'uid': 'root', 'pwd': 'peanut'}],
        [{'driver': '{MySQL ODBC 8.0 ANSI Driver}', 'server': 'localhost', 'database': 'sakila', 'uid': 'root', 'pwd': 'peanut'}],
        [{'driver': '{MySQL ODBC 8.0 ANSI Driver}', 'server': 'localhost', 'database': 'world', 'uid': 'root', 'pwd': 'peanut'}],
        [{'driver': '{MySQL ODBC 8.0 ANSI Driver}', 'server': 'localhost', 'database': 'tsqlv4', 'uid': 'root', 'pwd': 'peanut'}]
    ]

    @parameterized.expand(database_connections)
    def test_sql_software_connect(self, connect_dict):
        """
        Test - if all the database connections are successful using pyodbc package and sql_software_connect method.
        For testing new connections - enter a new connection dictionary into the database_connections list in the
        test_sqlpyodbcconnect script.
        """
        connect = sqldb.SQLSoftwareConnect(**connect_dict)
        cursor = connect.sql_software_connect()
        self.assertEqual(type(cursor), pyodbc.Cursor)

    @parameterized.expand([
        [database_connections[0][0], database_connections[1][0]],
        [database_connections[3][0], database_connections[4][0]]
    ])
    def test_change_multiple_variables(self, connect1, connect2):
        """
        Test - if the database resets are successful using the change_multiple_variables method.  For testing other
        variables - build new servers in all the SQL Software, then test to see if the change in the Server was
        successful (repeat process for uid, pwd).
        """
        start_database = connect1
        end_database = connect2

        connect = sqldb.SQLSoftwareConnect(**start_database)
        connect.change_multiple_variables(end_database)
        self.assertEqual(connect.database, end_database['database'])

if __name__ == '__main__':
    unittest.main()
