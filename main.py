# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# See PyCharm help at https://www.jetbrains.com/help/pycharm/

"""
:param str col_data_type: Starting SQL System column data type
:param int/None col_char_size: Starting SQL System column character size
:param int/None col_num_precision: Starting SQL System column numeric precision
:param int/None col_num_scale: Starting SQL System column numeric scale
:param int/None col_date_precision: Starting SQL System column date precision
:param str/None col_charset: Starting SQL System column charset

self.column_name = column_name
self.data_type = None
self.char_size = char_size
self.num_precision = num_precision
self.num_scale = num_scale
self.date_precision = date_precision
self.nullable = nullable
self.charset = charset

"""
from sqldbstructure.sqltable import SQLServerTable, MySQLTable
from sqldbstructure.sqlcolumn import SQLServerColumn, MySQLColumn
from sqldbstructure.sqldatabase import SQLServerDatabase, MySQLDatabase
from sqldbstructure.sqlengine import SQLServerEngine, MySQLEngine

sql_server = SQLServerEngine  # put connection information here
mysql = MySQLEngine           # put connection information here

tables = [
    'actor',
    'address',
    'category',
    'city',
    'country',
    'customer',
    'film',
    'film_actor',
    'film_category',
    'film_text',
    'inventory',
    'language',
    'payment',
    'rental',
    'staff',
    'store'
]

sql_server.sql_transfer_tables_from_mysql(sqlengine=mysql, tables=tables)

sql_server.connection.close()
mysql.connection.close()