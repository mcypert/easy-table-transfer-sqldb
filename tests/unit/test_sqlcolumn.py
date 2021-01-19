import unittest
from parameterized import parameterized
from sqldbstructure.sqlcolumn import MySQLColumn, SQLServerColumn
from sqldbconversion.sqldataconvert import MySQLConversion, SQLServerConversion

class SQLColumnTest(unittest.TestCase):

    @parameterized.expand([
        ['Test Convert Column SQL Server to MySQL', 'TestColumn', 'varchar', 100, None, None, None, 'yes', None, 'SQLServerColumn', 'MySQLEngine', MySQLConversion],
        ['Test Convert Column MySQL to SQL Server', 'TestColumn', 'varchar', 100, None, None, None, 'yes', None, 'MySQLColumn', 'SQLServerEngine', SQLServerConversion]

    ])
    def test_convert_column(self, test_name, column, data_type, char_size, num_precision, num_scale, date_precision, nullable, charset, start, end, result):
        """
        """
        column_object = globals()[start](column, data_type, char_size, num_precision, num_scale, date_precision, nullable, charset)
        column_convert = column_object.convert_column(end)
        self.assertEqual(type(column_convert), result, msg=f"{test_name} failed")

    @parameterized.expand([
        ['Test Format Column SQL Server', 'TestColumn', 'varchar', 100, None, None, None, 'yes', None, 'SQLServerColumn', 'TestColumn varchar(100) null'],
        ['Test Format Column MySQL', 'TestColumn', 'varchar', 100, None, None, None, 'yes', None, 'MySQLColumn', 'TestColumn varchar(100) character set utf8mb4 collate utf8mb4_unicode_ci null'],
    ])
    def test_format_column(self, test_name, column, data_type, char_size, num_precision, num_scale, date_precision, nullable, charset, start, result):
        """
        """
        column_object = globals()[start](column, data_type, char_size, num_precision, num_scale, date_precision, nullable, charset)
        column_format = column_object.format_column()
        self.assertEqual(column_format, result, msg=f"{test_name} failed")

    @parameterized.expand([
        ['Test Nullable Format SQL Server - yes', 'TestColumn', 'varchar', 100, None, None, None, 'yes', None, 'SQLServerColumn', 'null'],
        ['Test Nullable Format SQL Server - no', 'TestColumn', 'varchar', 100, None, None, None, 'no', None, 'SQLServerColumn', 'not null'],
        ['Test Nullable Format MySQL - yes', 'TestColumn', 'varchar', 100, None, None, None, 'yes', None, 'MySQLColumn', 'null'],
        ['Test Nullable Format MySQL - no', 'TestColumn', 'varchar', 100, None, None, None, 'no', None, 'MySQLColumn', 'not null']
    ])
    def test_format_nullable(self, test_name, column, data_type, char_size, num_precision, num_scale, date_precision, nullable, charset, start, result):
        """
        """
        column_object = globals()[start](column, data_type, char_size, num_precision, num_scale, date_precision, nullable, charset)
        column_nullable = column_object._format_nullable()
        self.assertEqual(column_nullable, result, msg=f"{test_name} failed")

if __name__ == '__main__':
    unittest.main()