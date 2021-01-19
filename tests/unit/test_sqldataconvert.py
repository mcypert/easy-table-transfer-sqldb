import unittest
from parameterized import parameterized
from sqldbconversion.sqldataconvert import SQLServerConversion, MySQLConversion
import tests.unit.test_conversionbuilder as thf

mysql_national_charset = [
    'big5',
    'ujis',
    'sjis',
    'euckr',
    'gb2312',
    'gbk',
    'utf8',
    'ucs2',
    'cp932',
    'eucjpms'
]

mysql_regular_charset = [
    'dec8',
    'cp850',
    'hp8',
    'koi8r',
    'latin 1',
    'latin2',
    'swe7',
    'ascii',
    'hebrew',
    'tis620',
    'koi8u',
    'greek',
    'cp 1256',
    'cp 1257',
    'cp 1250',
    'latin5',
    'armscii8',
    'cp866',
    'keybcs2',
    'macce',
    'macroman',
    'cp852',
    'latin7',
    'geostd8',
    'cp 1251',
    'binary'
]

myss_conversion_character = {

    'test_national_charset': {
        'data_type': 'char',
        'char_size': 10,
        'charset': mysql_national_charset,
        'test_data_type': 'nchar',
        'test_char_size': 10,
        'test_charset': None,
    },

    'test_regular_charset': {
        'data_type': 'char',
        'char_size': 10,
        'charset': mysql_regular_charset,
        'test_data_type': 'char',
        'test_char_size': 10,
        'test_charset': None,
    },

    'test_enum': {
        'data_type': 'enum',
        'char_size': 10,
        'charset': 'dec8',
        'test_data_type': 'varchar',
        'test_char_size': 255,
        'test_charset': None,
    },

    'test_text': {
        'data_type': 'text',
        'char_size': 3000,
        'charset': 'dec8',
        'test_data_type': 'varchar',
        'test_char_size': 'max',
        'test_charset': None,
    },

    'test_longtext': {
        'data_type': 'longtext',
        'char_size': 3000,
        'charset': 'dec8',
        'test_data_type': 'varchar',
        'test_char_size': 'max',
        'test_charset': None,
    },

    'test_mediumtext': {
        'data_type': 'mediumtext',
        'char_size': 2000,
        'charset': 'dec8',
        'test_data_type': 'varchar',
        'test_char_size': 'max',
        'test_charset': None,
    },

    'test_tinytext': {
        'data_type': 'tinytext',
        'char_size': 200,
        'charset': 'dec8',
        'test_data_type': 'varchar',
        'test_char_size': 255,
        'test_charset': None,
    },

    'test_varchar': {
        'data_type': 'varchar',
        'char_size': [1, 4000, 8000, 12000],
        'charset': 'dec8',
        'test_data_type': 'varchar',
        'test_char_size': [1, 4000, 8000, 'max'],
        'test_charset': None,
    },

    'test_char': {
        'data_type': 'char',
        'char_size': [1, 150, 255],
        'charset': 'dec8',
        'test_data_type': 'char',
        'test_char_size': [1, 150, 255],
        'test_charset': None,
    },

    'test_binary': {
        'data_type': 'binary',
        'char_size': 500,
        'charset': None,
        'test_data_type': 'binary',
        'test_char_size': 500,
        'test_charset': None,
    },

    'test_blob': {
        'data_type': 'blob',
        'char_size': None,
        'charset': None,
        'test_data_type': 'varbinary',
        'test_char_size': 'max',
        'test_charset': None,
    },

    'test_longblob': {
        'data_type': 'longblob',
        'char_size': None,
        'charset': None,
        'test_data_type': 'varbinary',
        'test_char_size': 'max',
        'test_charset': None,
    },

    'test_mediumblob': {
        'data_type': 'mediumblob',
        'char_size': None,
        'charset': None,
        'test_data_type': 'varbinary',
        'test_char_size': 'max',
        'test_charset': None,
    },

    'test_tinyblob': {
        'data_type': 'tinyblob',
        'char_size': None,
        'charset': None,
        'test_data_type': 'varbinary',
        'test_char_size': 255,
        'test_charset': None,
    },

    'test_varbinary': {
        'data_type': 'varbinary',
        'char_size': 250,
        'charset': None,
        'test_data_type': 'varbinary',
        'test_char_size': 250,
        'test_charset': None,
    },
}

myss_conversion_numeric = {

    'test_bit': {
        'data_type': 'bit',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'numeric',
        'test_num_precision': 20,
        'test_num_scale': 0,
    },

    'test_int': {
        'data_type': 'int',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'bigint',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_integer': {
        'data_type': 'integer',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'bigint',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_bigint': {
        'data_type': 'bigint',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'numeric',
        'test_num_precision': 20,
        'test_num_scale': 0,
    },

    'test_mediumint': {
        'data_type': 'mediumint',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'int',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_smallint': {
        'data_type': 'mediumint',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'int',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_tinyint': {
        'data_type': 'tinyint',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'int',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_decimal': {
        'data_type': 'decimal',
        'num_precision': [10, 32, 39, 45],
        'num_scale': [0, 2, 10, 20],
        'test_data_type': 'numeric',
        'test_num_precision': [10, 32, 38, 38],
        'test_num_scale': [0, 2, 10, 20],
    },

    'test_numeric': {
        'data_type': 'numeric',
        'num_precision': [10, 32, 39, 45],
        'num_scale': [0, 2, 10, 20],
        'test_data_type': 'numeric',
        'test_num_precision': [10, 32, 38, 38],
        'test_num_scale': [0, 2, 10, 20],
    },

    'test_double': {
        'data_type': 'double',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'float',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_double_precision': {
        'data_type': 'double precision',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'double precision',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_float': {
        'data_type': 'float',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'real',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_year': {
        'data_type': 'year',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'int',
        'test_num_precision': None,
        'test_num_scale': None,
    },
}

myss_conversion_datetime = {

    'test_date': {
        'data_type': 'date',
        'date_precision': None,
        'test_data_type': 'date',
        'test_date_precision': None,
    },

    'test_datetime': {
        'data_type': 'datetime',
        'date_precision': [-1, 5],
        'test_data_type': 'datetime2',
        'test_date_precision': [0, 5],
    },

    'test_time': {
        'data_type': 'time',
        'date_precision': [-1, 5],
        'test_data_type': 'time',
        'test_date_precision': [0, 5],
    },

    'test_timestamp': {
        'data_type': 'timestamp',
        'date_precision': [-1, 5],
        'test_data_type': 'datetime2',
        'test_date_precision': [0, 5],
    },
}

class SQLServerToMySQLTest(unittest.TestCase):

    @parameterized.expand(
        thf.test_case_list("build_character_test", myss_conversion_character)
    )
    def test_character_convert_mysql_to_sqlserver(self, test_parameters):
        """mysql to sql server character conversion
        """
        conversion_object = SQLServerConversion(**test_parameters[1])
        conversion_object.mysql_conversion()

        with self.subTest():
            self.assertEqual(conversion_object.data_type, test_parameters[2], msg=f"{test_parameters[0]}")

        with self.subTest():
            self.assertEqual(conversion_object.char_size, test_parameters[3], msg=f"{test_parameters[0]}")

    @parameterized.expand(
        thf.test_case_list("build_numeric_test", myss_conversion_numeric)
    )
    def test_numeric_convert_mysql_to_sqlserver(self, test_parameters):
        """mysql to sql server numeric
        """
        conversion_object = SQLServerConversion(**test_parameters[1])
        conversion_object.mysql_conversion()

        with self.subTest():
            self.assertEqual(conversion_object.data_type, test_parameters[2], msg=f"{test_parameters[0]}")

        with self.subTest():
            self.assertEqual(conversion_object.num_precision, test_parameters[3], msg=f"{test_parameters[0]}")

        with self.subTest():
            self.assertEqual(conversion_object.num_scale, test_parameters[4], msg=f"{test_parameters[0]}")

    @parameterized.expand(
        thf.test_case_list("build_datetime_test", myss_conversion_datetime)
    )
    def test_datetime_convert_mysql_to_sqlserver(self, test_parameters):
        """mysql to sql server character conversion
        """
        conversion_object = SQLServerConversion(**test_parameters[1])
        conversion_object.mysql_conversion()

        with self.subTest():
            self.assertEqual(conversion_object.data_type, test_parameters[2], msg=f"{test_parameters[0]}")

        with self.subTest():
            self.assertEqual(conversion_object.date_precision, test_parameters[3], msg=f"{test_parameters[0]}")

ssmy_conversion_character = {

    'test_char': {
        'data_type': 'char',
        'char_size': [10, 125, 255, 500, 7999],
        'charset': None,
        'test_data_type': ['char', 'char', 'char', 'varchar', 'varchar'],
        'test_char_size': [10, 125, 255, 500, 7999],
        'test_charset': None,
    },

    'test_nchar': {
        'data_type': 'char',
        'char_size': [10, 125, 255, 500, 7999],
        'charset': None,
        'test_data_type': ['char', 'char', 'char', 'varchar', 'varchar'],
        'test_char_size': [10, 125, 255, 500, 7999],
        'test_charset': None,
    },

    'test_varchar': {
        'data_type': 'varchar',
        'char_size': [-1, None],
        'charset': None,
        'test_data_type': ['longtext', 'varchar'],
        'test_char_size': [-1, None],
        'test_charset': None,
    },

    'test_nvarchar': {
        'data_type': 'nvarchar',
        'char_size': [-1, None],
        'charset': None,
        'test_data_type': ['longtext', 'varchar'],
        'test_char_size': [-1, None],
        'test_charset': None,
    },

    'test_text': {
        'data_type': 'text',
        'char_size': None,
        'charset': None,
        'test_data_type': 'text',
        'test_char_size': None,
        'test_charset': None,
    },

    'test_ntext': {
        'data_type': 'text',
        'char_size': None,
        'charset': None,
        'test_data_type': 'text',
        'test_char_size': None,
        'test_charset': None,
    },

    'test_binary': {
        'data_type': 'binary',
        'char_size': [-1, 100, 255, 5000],
        'charset': None,
        'test_data_type': ['blob', 'binary', 'binary', 'blob'],
        'test_char_size': [-1, 100, 255, 5000],
        'test_charset': None,
    },

    'test_varbinary': {
        'data_type': 'varbinary',
        'char_size': [-1, 5000],
        'charset': None,
        'test_data_type': ['longblob', 'varchar'],
        'test_char_size': [-1, 5000],
        'test_charset': None,
    },
}

ssmy_conversion_numeric = {

    'test_bit': {
        'data_type': 'bit',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'tinyint',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_tinyint': {
        'data_type': 'tinyint',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'smallint',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_smallint': {
        'data_type': 'smallint',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'smallint',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_bigint': {
        'data_type': 'bigint',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'bigint',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_int': {
        'data_type': 'int',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'int',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_decimal': {
        'data_type': 'decimal',
        'num_precision': [12, 12, 40, 36, 45],
        'num_scale': [10, 12, 22, -1, -1],
        'test_data_type': 'decimal',
        'test_num_precision': [12, 12, 18, 36, 18],
        'test_num_scale': [10, 12, 0, 0, 0],
    },

    'test_numeric': {
        'data_type': 'numeric',
        'num_precision': [12, 12, 40, 36, 45],
        'num_scale': [10, 12, 22, -1, -1],
        'test_data_type': 'decimal',
        'test_num_precision': [12, 12, 18, 36, 18],
        'test_num_scale': [10, 12, 0, 0, 0],
    },

    'test_float': {
        'data_type': 'float',
        'num_precision': [12, 24, 36],
        'num_scale': None,
        'test_data_type': ['float', 'float', 'double'],
        'test_num_precision': [12, 24, 36],
        'test_num_scale': None,
    },

    'test_real': {
        'data_type': 'real',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'float',
        'test_num_precision': None,
        'test_num_scale': None,
    },

    'test_money': {
        'data_type': 'money',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'decimal',
        'test_num_precision': 19,
        'test_num_scale': 4,
    },

    'test_smallmoney': {
        'data_type': 'smallmoney',
        'num_precision': None,
        'num_scale': None,
        'test_data_type': 'decimal',
        'test_num_precision': 10,
        'test_num_scale': 4,
    },
}

ssmy_conversion_datetime = {

    'test_date': {
        'data_type': 'date',
        'date_precision': None,
        'test_data_type': 'date',
        'test_date_precision': None,
    },

    'test_datetime': {
        'data_type': 'datetime',
        'date_precision': None,
        'test_data_type': 'datetime',
        'test_date_precision': None,
    },

    'test_datetime2': {
        'data_type': 'datetime',
        'date_precision': None,
        'test_data_type': 'datetime',
        'test_date_precision': None,
    },

    'test_datetimeoffset': {
        'data_type': 'datetime',
        'date_precision': None,
        'test_data_type': 'datetime',
        'test_date_precision': None,
    },

    'test_smalldatetime': {
        'data_type': 'smalldatetime',
        'date_precision': None,
        'test_data_type': 'datetime',
        'test_date_precision': None,
    },

    'test_time': {
        'data_type': 'time',
        'date_precision': [0, 4, 7],
        'test_data_type': 'time',
        'test_date_precision': [0, 4, 7],
    },

    'test_timestamp': {
        'data_type': 'timestamp',
        'date_precision': None,
        'test_data_type': 'bigint',
        'test_date_precision': None,
    },
}

class MySQLToSQLServerTest(unittest.TestCase):

    @parameterized.expand(
        thf.test_case_list("build_character_test", ssmy_conversion_character)
    )
    def test_character_convert_sqlserver_to_mysql(self, test_parameters):
        """sql server to mysql character conversion
        """
        conversion_object = MySQLConversion(**test_parameters[1])
        conversion_object.sqlserver_conversion()

        with self.subTest():
            self.assertEqual(conversion_object.data_type, test_parameters[2], msg=f"{test_parameters[0]}")

        with self.subTest():
            self.assertEqual(conversion_object.char_size, test_parameters[3], msg=f"{test_parameters[0]}")

    @parameterized.expand(
        thf.test_case_list("build_numeric_test", ssmy_conversion_numeric)
    )
    def test_numeric_convert_sqlserver_to_mysql(self, test_parameters):
        """sql server to mysql numeric conversion
        """
        conversion_object = MySQLConversion(**test_parameters[1])
        conversion_object.sqlserver_conversion()

        with self.subTest():
            self.assertEqual(conversion_object.data_type, test_parameters[2], msg=f"{test_parameters[0]}")

        with self.subTest():
            self.assertEqual(conversion_object.num_precision, test_parameters[3], msg=f"{test_parameters[0]}")

        with self.subTest():
            self.assertEqual(conversion_object.num_scale, test_parameters[4], msg=f"{test_parameters[0]}")

    @parameterized.expand(
        thf.test_case_list("build_datetime_test", ssmy_conversion_datetime)
    )
    def test_datetime_convert_sqlserver_to_mysql(self, test_parameters):
        """sql server to mysql character conversion
        """
        conversion_object = MySQLConversion(**test_parameters[1])
        conversion_object.sqlserver_conversion()

        with self.subTest():
            self.assertEqual(conversion_object.data_type, test_parameters[2], msg=f"{test_parameters[0]}")

        with self.subTest():
            self.assertEqual(conversion_object.date_precision, test_parameters[3], msg=f"{test_parameters[0]}")

if __name__ == '__main__':
    unittest.main()