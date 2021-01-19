from abc import ABC, abstractmethod
import sqldbconversion.convertexcept as ex
import helperfunctions as hf

class SQLConversion(ABC):

    def __init__(self, data_type, char_size, num_precision, num_scale, date_precision, charset):
        self.column = None
        self.data_type = data_type
        self.char_size = char_size
        self.num_precision = num_precision
        self.num_scale = num_scale
        self.date_precision = date_precision
        self.nullable = None
        self.charset = charset

    def _character(self, parameter_dict):
        data_type = parameter_dict['data_type']
        char_size = parameter_dict['char_size']
        charset = parameter_dict['charset']

        self.data_type = data_type
        self.char_size = char_size
        self.charset = charset

    def _numeric(self, parameter_dict):
        data_type = parameter_dict['data_type']
        num_precision = parameter_dict['num_precision']
        num_scale = parameter_dict['num_scale']

        self.data_type = data_type
        self.num_precision = num_precision
        self.num_scale = num_scale

    def _datetime(self, parameter_dict):
        data_type = parameter_dict['data_type']
        date_precision = parameter_dict['date_precision']

        self.data_type = data_type
        self.date_precision = date_precision

    def _binary(self, parameter_dict):
        data_type = parameter_dict['data_type']
        char_size = parameter_dict['char_size']

        self.data_type = data_type
        self.char_size = char_size

    @abstractmethod
    def __repr__(self):
        pass

class SQLServerConversion(SQLConversion):

    _MYSQL_CHARSET = ['big5', 'ujis', 'sjis', 'euckr', 'gb2312', 'gbk', 'utf8', 'ucs2', 'cp932', 'eucjpms', 'utf8mb4']

    def __init__(self, data_type, char_size, num_precision, num_scale, date_precision, charset):
        super().__init__(data_type, char_size, num_precision, num_scale, date_precision, charset)

    def mysql_conversion(self):
        conversion = {
            'char': (self._mysql_character_version_1, {'data_type': 'char', 'char_size': self.char_size, 'charset': self.charset}),
            'enum': (self._mysql_character_version_1, {'data_type': 'varchar', 'char_size': 255, 'charset': self.charset}),
            'longtext': (self._mysql_character_version_1, {'data_type': 'varchar', 'char_size': 'max', 'charset': self.charset}),
            'mediumtext': (self._mysql_character_version_1, {'data_type': 'varchar', 'char_size': 'max', 'charset': self.charset}),
            'set': (self._mysql_character_version_1, {'data_type': 'varchar', 'char_size': 4000, 'charset': self.charset}),
            'text': (self._mysql_character_version_1, {'data_type': 'varchar', 'char_size': 'max', 'charset': self.charset}),
            'tinytext': (self._mysql_character_version_1, {'data_type': 'varchar', 'char_size': 255, 'charset': self.charset}),
            'varchar': (self._mysql_character_version_2, {'data_type': 'varchar', 'char_size': self.char_size, 'charset': self.charset}),
            'bigint': (self._numeric, {'data_type': 'numeric', 'num_precision': 20, 'num_scale': 0}),
            'bit': (self._numeric, {'data_type': 'numeric', 'num_precision': 20, 'num_scale': 0}),
            'decimal': (self._mysql_numeric_version_1, {'data_type': 'numeric', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'numeric': (self._mysql_numeric_version_1, {'data_type': 'numeric', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'double': (self._numeric, {'data_type': 'float', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'double precision': (self._numeric, {'data_type': 'double precision', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'float': (self._numeric, {'data_type': 'real', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'int': (self._numeric, {'data_type': 'bigint', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'integer': (self._numeric, {'data_type': 'bigint', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'mediumint': (self._numeric, {'data_type': 'int', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'smallint': (self._numeric, {'data_type': 'int', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'tinyint': (self._numeric, {'data_type': 'int', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'date': (self._datetime, {'data_type': 'date', 'date_precision': self.date_precision}),
            'datetime': (self._mysql_datetime_version_1, {'data_type': 'datetime2', 'date_precision': self.date_precision}),
            'time': (self._mysql_datetime_version_1, {'data_type': 'time', 'date_precision': self.date_precision}),
            'timestamp': (self._mysql_datetime_version_1, {'data_type': 'datetime2', 'date_precision': self.date_precision}),
            'year': (self._datetime, {'data_type': 'int', 'date_precision': self.date_precision}),
            'binary': (self._binary, {'data_type': 'binary', 'char_size': self.char_size}),
            'blob': (self._binary, {'data_type': 'varbinary', 'char_size': 'max'}),
            'longblob': (self._binary, {'data_type': 'varbinary', 'char_size': 'max'}),
            'mediumblob': (self._binary, {'data_type': 'varbinary', 'char_size': 'max'}),
            'tinyblob': (self._binary, {'data_type': 'varbinary', 'char_size': 255}),
            'varbinary': (self._mysql_binary_version_1, {'data_type': 'varbinary', 'char_size': self.char_size})
        }

        conversion_tuple = conversion.get(self.data_type)
        if conversion_tuple is None:
            raise ex.SQLConversionError(f'{self.data_type} data type conversion is not supported.')

        function, parameters = conversion_tuple[0], conversion_tuple[1]
        function(parameters)

    def _mysql_character_version_1(self, parameter_dict):
        """
        Applies to char, enum, longtext, mediumtext, set, text, tinytext
        """
        data_type = parameter_dict['data_type']
        char_size = parameter_dict['char_size']
        charset = parameter_dict['charset']

        if charset in SQLServerConversion._MYSQL_CHARSET:
            self.data_type, self.char_size, self.charset = f'n{data_type}', char_size, charset
        else:
            self.data_type, self.char_size, self.charset = f'{data_type}', char_size, charset

    def _mysql_character_version_2(self, parameter_dict):
        """
        Applies to varchar
        """
        data_type = parameter_dict['data_type']
        char_size = parameter_dict['char_size']
        charset = parameter_dict['charset']

        if charset in SQLServerConversion._MYSQL_CHARSET:
            if char_size > 8000:
                self.data_type, self.char_size, self.charset = f'n{data_type}', 'max', charset
            else:
                self.data_type, self.char_size, self.charset = f'n{data_type}', char_size, charset
        else:
            if char_size > 8000:
                self.data_type, self.char_size, self.charset = f'{data_type}', 'max', charset
            else:
                self.data_type, self.char_size, self.charset = f'{data_type}', char_size, charset

    def _mysql_numeric_version_1(self, parameter_dict):
        """
        Applies to decimal
        """
        data_type = parameter_dict['data_type']
        num_precision = parameter_dict['num_precision']
        num_scale = parameter_dict['num_scale']

        if num_precision > 38:
            self.data_type = data_type
            self.num_precision = 38
            self.num_scale = num_scale
        else:
            self.data_type = data_type
            self.num_precision = num_precision
            self.num_scale = num_scale

    def _mysql_datetime_version_1(self, parameter_dict):
        """
        Applies to datetime, time, timestamp
        """
        data_type = parameter_dict['data_type']
        date_precision = parameter_dict['date_precision']

        if date_precision == -1:
            self.data_type = data_type
            self.date_precision = 0
        else:
            self.data_type = data_type
            self.date_precision = date_precision

    def _mysql_binary_version_1(self, parameter_dict):
        """
        Applies to varbinary
        """
        data_type = parameter_dict['data_type']
        char_size = parameter_dict['char_size']

        if char_size > 8000:
            self.data_type = f'{data_type}'
            self.char_size = 'max'
        else:
            self.data_type = f'{data_type}'
            self.char_size = char_size

    def __repr__(self):
        return f'{__class__.__name__}(' \
               f'{hf.add_quotes(self.data_type)}, ' \
               f'{self.char_size}, ' \
               f'{self.num_precision}, ' \
               f'{self.num_scale}, ' \
               f'{self.date_precision}, ' \
               f'{hf.add_quotes(self.charset)})'

class MySQLConversion(SQLConversion):

    def __init__(self, data_type, char_size, num_precision, num_scale, date_precision, charset):
        super().__init__(data_type, char_size, num_precision, num_scale, date_precision, charset)

    def sqlserver_conversion(self):
        conversion = {
            'char': (self._sqlserver_character_version_1, {'data_type': 'char', 'char_size': self.char_size, 'charset': self.charset}),
            'nchar': (self._sqlserver_character_version_1, {'data_type': 'char', 'char_size': self.char_size, 'charset': 'utf8mb4_unicode_ci'}),
            'ntext': (self._character, {'data_type': 'text', 'char_size': self.char_size, 'charset': 'utf8mb4_unicode_ci'}),
            'nvarchar': (self._sqlserver_character_version_2, {'data_type': 'varchar', 'char_size': self.char_size, 'charset': 'utf8mb4_unicode_ci'}),
            'text': (self._character, {'data_type': 'text', 'char_size': self.char_size, 'charset': self.charset}),
            'varchar': (self._sqlserver_character_version_2, {'data_type': 'varchar', 'char_size': self.char_size, 'charset': self.charset}),
            'bigint': (self._numeric, {'data_type': 'bigint', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'bit': (self._numeric, {'data_type': 'tinyint', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'decimal': (self._sqlserver_numeric_version_1, {'data_type': 'decimal', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'float': (self._sqlserver_numeric_version_2, {'data_type': 'float', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'int': (self._numeric, {'data_type': 'int', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'integer': (self._numeric, {'data_type': 'int', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'money': (self._numeric, {'data_type': 'decimal', 'num_precision': 19, 'num_scale': 4}),
            'numeric': (self._sqlserver_numeric_version_1, {'data_type': 'decimal', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'real': (self._numeric, {'data_type': 'float', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'smallint': (self._numeric, {'data_type': 'smallint', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'smallmoney': (self._numeric, {'data_type': 'decimal', 'num_precision': 10, 'num_scale': 4}),
            'tinyint': (self._numeric, {'data_type': 'smallint', 'num_precision': self.num_precision, 'num_scale': self.num_scale}),
            'date': (self._datetime, {'data_type': 'date', 'date_precision': self.date_precision}),
            'datetime': (self._datetime, {'data_type': 'datetime', 'date_precision': self.date_precision}),
            'datetime2': (self._datetime, {'data_type': 'datetime', 'date_precision': self.date_precision}),
            'datetimeoffset': (self._datetime, {'data_type': 'datetime', 'date_precision': self.date_precision}),
            'smalldatetime': (self._datetime, {'data_type': 'datetime', 'date_precision': self.date_precision}),
            'time': (self._datetime, {'data_type': 'time', 'date_precision': self.date_precision}),
            'timestamp': (self._datetime, {'data_type': 'bigint', 'date_precision': self.date_precision}),
            'binary': (self._sqlserver_binary_version_1, {'data_type': 'blob', 'char_size': self.char_size})
            # 'varbinary': (self._sqlserver_binary_version_2, {'data_type': 'longblob', 'char_size': self.char_size})
        }

        conversion_tuple = conversion.get(self.data_type)
        if conversion_tuple is None:
            conversion_string = f'{self.data_type} data type conversion is not supported.'
            raise ex.SQLConversionError(conversion_string)

        function, parameters = conversion_tuple[0], conversion_tuple[1]
        function(parameters)

    def _sqlserver_character_version_1(self, parameters):
        """
        Applies to char, nchar
        """
        data_type = parameters['data_type']
        char_size = parameters['char_size']
        charset = parameters['charset']

        if char_size <= 255:
            self.data_type = data_type
            self.char_size = char_size
            self.charset = charset
        else:
            self.data_type = 'varchar'
            self.char_size = char_size
            self.charset = charset

    def _sqlserver_character_version_2(self, parameters):
        """
        Applies to varchar, nvarchar
        """
        data_type = parameters['data_type']
        char_size = parameters['char_size']
        charset = parameters['charset']

        if char_size == -1:
            self.data_type = 'longtext'
            self.char_size = char_size
            self.charset = charset
        else:
            self.data_type = data_type
            self.char_size = char_size
            self.charset = charset

    def _sqlserver_numeric_version_1(self, parameters):
        """
        Applies to numeric, decimal
        """
        data_type = parameters['data_type']
        num_precision = parameters['num_precision']
        num_scale = parameters['num_scale']

        if (num_precision <= 38) and (num_scale <= num_precision) and (num_scale >= 0) and (num_precision >= 1):
            self.data_type = data_type
            self.num_precision = num_precision
            self.num_scale = num_scale
        elif (num_precision <= 38) and (num_scale == -1) and (num_precision >= 1):
            self.data_type = data_type
            self.num_precision = num_precision
            self.num_scale = 0
        else:
            self.data_type = data_type
            self.num_precision = 18
            self.num_scale = 0

    def _sqlserver_numeric_version_2(self, parameters):
        """
        Applies to float
        """
        data_type = parameters['data_type']
        num_precision = parameters['num_precision']
        num_scale = parameters['num_scale']

        if num_precision <= 24:
            self.data_type = data_type
            self.num_precision = num_precision
            self.num_scale = num_scale
        else:
            self.data_type = 'double'
            self.num_precision = num_precision
            self.num_scale = num_scale

    def _sqlserver_binary_version_1(self, parameters):
        """
        """
        data_type = parameters['data_type']
        char_size = parameters['char_size']

        if (char_size <= 255) and (char_size != -1):
            self.data_type = 'binary'
            self.char_size = char_size
        else:
            self.data_type = data_type
            self.char_size = char_size

    def _sqlserver_binary_version_2(self, parameters):
        """
        """
        data_type = parameters['data_type']
        char_size = parameters['char_size']

        if char_size != -1:
            self.data_type = 'varchar'
            self.char_size = char_size
        else:
            self.data_type = data_type
            self.char_size = char_size

    def __repr__(self):
        return f'{__class__.__name__}(' \
               f'{hf.add_quotes(self.data_type)}, ' \
               f'{self.char_size}, ' \
               f'{self.num_precision}, ' \
               f'{self.num_scale}, ' \
               f'{self.date_precision}, ' \
               f'{hf.add_quotes(self.charset)})'