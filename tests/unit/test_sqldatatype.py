import unittest
from sqldbstructure.sqldatatype import SQLServerDataType, MySQLDataType
from parameterized import parameterized

class SQLServerDataTypeFormatTest(unittest.TestCase):

    @parameterized.expand([
        ['test_bigint', 'bigint', None, None, None, None, 'bigint'],
        ['test_bit', 'bit', None, None, None, None, 'bit'],
        ['test_float', 'float', None, None, None, None, 'float'],
        ['test_double_precision', 'double precision', None, None, None, None, 'double precision'],
        ['test_double', 'double', None, None, None, None, 'double'],
        ['test_int', 'int', None, None, None, None, 'int'],
        ['test_integer', 'integer', None, None, None, None, 'integer'],
        ['test_money', 'money', None, None, None, None, 'money'],
        ['test_real', 'real', None, None, None, None, 'real'],
        ['test_smallint', 'smallint', None, None, None, None, 'smallint'],
        ['test_smallmoney', 'smallmoney', None, None, None, None, 'smallmoney'],
        ['test_tinyint', 'tinyint', None, None, None, None, 'tinyint'],
        ['test_decimal', 'decimal', None, 10, 2, None, "decimal(10, 2)"],
        ['test_numeric', 'numeric', None, 10, 2, None, "numeric(10, 2)"],
        ['test_ntext', 'ntext', None, None, None, None, 'ntext'],
        ['test_text', 'text', None, None, None, None, 'text'],
        ['test_binary', 'binary', 100, None, None, None, "binary(100)"],
        ['test_char', 'char', 100, None, None, None, "char(100)"],
        ['test_nchar', 'nchar', 100, None, None, None, "nchar(100)"],
        ['test_varchar', 'varchar', 100, None, None, None, "varchar(100)"],
        ['test_nvarchar', 'nvarchar', 100, None, None, None, "nvarchar(100)"],
        ['test_varbinary', 'varbinary', 100, None, None, None, "varbinary(100)"],
        ['test_date', 'date', None, None, None, None, 'date'],
        ['test_datetime', 'datetime', None, None, None, None, 'datetime'],
        ['test_smalldatetime', 'smalldatetime', None, None, None, None, 'smalldatetime'],
        ['test_timestamp', 'timestamp', None, None, None, None, 'timestamp'],
        ['test_datetime2', 'datetime2', None, None, None, 1, 'datetime2(1)'],
        ['test_datetimeoffset', 'datetimeoffset', None, None, None, 1, 'datetimeoffset(1)'],
        ['test_time', 'time', None, None, None, 1, 'time(1)'],
        ['test_cursor', 'cursor', None, None, None, None, 'cursor'],
        ['test_geography', 'geography', None, None, None, None, 'geography'],
        ['test_geometry', 'geometry', None, None, None, None, 'geometry'],
        ['test_hierarchyid', 'hierarchyid', None, None, None, None, 'hierarchyid'],
        ['test_image', 'image', None, None, None, None, 'image'],
        ['test_rowversion', 'rowversion', None, None, None, None, 'rowversion'],
        ['test_sql_variant', 'sql_variant', None, None, None, None, 'sql_variant'],
        ['test_sysname', 'sysname', None, None, None, None, 'sysname'],
        ['test_table', 'table', None, None, None, None, 'table'],
        ['test_uniqueidentifier', 'uniqueidentifier', None, None, None, None, 'uniqueidentifier'],
        ['test_xml', 'xml', None, None, None, None, 'xml'],
    ])
    def test_sqlserver_data_format(self, test_name, data_type, char_size, num_precision, num_scale, date_precision, test_result):
        """
        """
        data_object = SQLServerDataType(data_type, char_size, num_precision, num_scale, date_precision)
        data_format = data_object.data_type_format()
        self.assertEqual(data_format, test_result, msg=f"{test_name}")

class MySQLDataTypeFormatTest(unittest.TestCase):

    @parameterized.expand([
        ['test_bigint', 'bigint', None, None, None, None, 'bigint'],
        ['test_mediumint', 'mediumint', None, None, None, None, 'mediumint'],
        ['test_smallint', 'smallint', None, None, None, None, 'smallint'],
        ['test_tinyint', 'tinyint', None, None, None, None, 'tinyint'],
        ['test_bit', 'bit', None, None, None, None, 'bit'],
        ['test_double_precision', 'double precision', None, None, None, None, 'double precision'],
        ['test_double', 'double', None, None, None, None, 'double'],
        ['test_float', 'float', None, None, None, None, 'float'],
        ['test_real', 'real', None, None, None, None, 'real'],
        ['test_int', 'int', None, None, None, None, 'int'],
        ['test_year', 'year', None, None, None, None, 'year'],
        ['test_integer', 'integer', None, None, None, None, 'integer'],
        ['test_decimal', 'decimal', None, 10, 2, None, "decimal(10, 2)"],
        ['test_numeric', 'numeric', None, 10, 2, None, "numeric(10, 2)"],
        ['test_year', 'year', None, None, None, None, 'year'],
        ['test_date', 'date', None, None, None, None, 'date'],
        ['test_datetime', 'datetime', None, None, None, 5, 'datetime(5)'],
        ['test_time', 'time', None, None, None, 1, 'time(1)'],
        ['test_timestamp', 'timestamp', None, None, None, 5, 'timestamp(5)'],
        ['test_blob', 'blob', None, None, None, None, 'blob'],
        ['test_enum', 'enum', None, None, None, None, 'enum'],
        ['test_longtext', 'longtext', None, None, None, None, 'longtext'],
        ['test_mediumblob', 'mediumblob', None, None, None, None, 'mediumblob'],
        ['test_mediumtext', 'mediumtext', None, None, None, None, 'mediumtext'],
        ['test_set', 'set', None, None, None, None, 'set'],
        ['test_text', 'text', None, None, None, None, 'text'],
        ['test_tinyblob', 'tinyblob', None, None, None, None, 'tinyblob'],
        ['test_longblob', 'longblob', None, None, None, None, 'longblob'],
        ['test_tinytext', 'tinytext', None, None, None, None, 'tinytext'],
        ['test_binary', 'binary', 100, None, None, None, "binary(100)"],
        ['test_char', 'char', 100, None, None, None, "char(100)"],
        ['test_varbinary', 'varbinary', 100, None, None, None, "varbinary(100)"],
        ['test_varchar', 'varchar', 100, None, None, None, "varchar(100)"],
        ['test_geometrycollection', 'geometrycollection', None, None, None, None, 'geometrycollection'],
        ['test_geometry', 'geometry', None, None, None, None, 'geometry'],
        ['test_linestring', 'linestring', None, None, None, None, 'linestring'],
        ['test_multipoint', 'multipoint', None, None, None, None, 'multipoint'],
        ['test_multipolygon', 'multipolygon', None, None, None, None, 'multipolygon'],
        ['test_multistring', 'multistring', None, None, None, None, 'multistring'],
        ['test_point', 'point', None, None, None, None, 'point'],
        ['test_ploygon', 'json', None, None, None, None, 'json']

    ])
    def test_mysql_data_format(self, test_name, data_type, char_size, num_precision, num_scale, date_precision, test_result):
        """
        """
        data_object = MySQLDataType(data_type, char_size, num_precision, num_scale, date_precision)
        data_format = data_object.data_type_format()
        self.assertEqual(data_format, test_result, msg=f"{test_name}")

if __name__ == '__main__':
    unittest.main()
