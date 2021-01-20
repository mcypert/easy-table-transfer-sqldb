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

# sql_server = SQLServerEngine  # put connection information here
# mysql = MySQLEngine           # put connection information here

sql_server = SQLServerEngine
mysql = MySQLEngine

statement = f"""
    select first_name, last_name, title
    from sakila.rental as t1
    inner join sakila.customer as t2 on t1.customer_id = t2.customer_id
    inner join sakila.inventory as t3 on t1.inventory_id = t3.inventory_id
    inner join sakila.film as t4 on t3.film_id = t4.film_id
"""

# mysql.sql_create_custom_table(query_statement=statement, database='sakila', table='Tran_Table', drop_table=True)

tables = [
    'tran_table',
    'store'
]

sql_server.sql_transfer_tables_from_mysql(sqlengine=mysql, tables=tables)
sql_server.connection.close()
mysql.connection.close()