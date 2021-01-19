from abc import ABC, abstractmethod
import sqldbstructure.structureexcept as ex

class SQLDataType(ABC):

    def __init__(self, data_type, char_size, num_precision, num_scale, date_precision):
        self.data_type = data_type
        self.char_size = char_size
        self.num_precision = num_precision
        self.num_scale = num_scale
        self.date_precision = date_precision

    def _character_format_1(self):
        return f"{self.data_type}"

    def _character_format_2(self):
        return f"{self.data_type}({self.char_size})"

    def _numeric_format_1(self):
        return f"{self.data_type}"

    def _numeric_format_2(self):
        return f"{self.data_type}({self.num_precision}, {self.num_scale})"

    def _datetime_format_1(self):
        return f"{self.data_type}"

    def _datetime_format_2(self):
        return f"{self.data_type}({self.date_precision})"

    @abstractmethod
    def __repr__(self):
        pass

class SQLServerDataType(SQLDataType):

    def __init__(self, data_type, char_size, num_precision, num_scale, date_precision):
        super().__init__(data_type, char_size, num_precision, num_scale, date_precision)

    def data_type_format(self):
        data_types = {
            'bigint': self._numeric_format_1,
            'binary': self._character_format_2,
            'bit': self._numeric_format_1,
            'char': self._character_format_2,
            'cursor': self._other_format_1,
            'date': self._datetime_format_1,
            'datetime': self._datetime_format_1,
            'datetime2': self._datetime_format_2,
            'datetimeoffset': self._datetime_format_2,
            'decimal': self._numeric_format_2,
            'double': self._numeric_format_1,
            'double precision': self._numeric_format_1,
            'float': self._numeric_format_1,
            'geography': self._other_format_1,
            'geometry': self._other_format_1,
            'hierarchyid': self._other_format_1,
            'image': self._other_format_1,
            'int': self._numeric_format_1,
            'integer': self._numeric_format_1,
            'money': self._numeric_format_1,
            'nchar': self._character_format_2,
            'ntext': self._character_format_1,
            'numeric': self._numeric_format_2,
            'nvarchar': self._character_format_2,
            'real': self._numeric_format_1,
            'rowversion': self._other_format_1,
            'smalldatetime': self._datetime_format_1,
            'smallint': self._numeric_format_1,
            'smallmoney': self._numeric_format_1,
            'sql_variant': self._other_format_1,
            'sysname': self._other_format_1,
            'table': self._other_format_1,
            'text': self._character_format_1,
            'time': self._datetime_format_2,
            'timestamp': self._datetime_format_1,
            'tinyint': self._numeric_format_1,
            'uniqueidentifier': self._other_format_1,
            'varbinary': self._character_format_2,
            'varchar': self._character_format_2,
            'xml': self._other_format_1,
        }

        format_data_type = data_types.get(self.data_type)

        if format_data_type is None:
            raise ex.SQLDataTypeError(f'{self.data_type} data type is not listed as a SQL Server Data Type.')
        return format_data_type()

    def _other_format_1(self):
        return f"{self.data_type}"

    def __repr__(self):
        return f"{__class__.__name__}()"

class MySQLDataType(SQLDataType):

    def __init__(self, data_type, char_size, num_precision, num_scale, date_precision):
        super().__init__(data_type, char_size, num_precision, num_scale, date_precision)

    def data_type_format(self):
        data_types = {
            'bigint': self._numeric_format_1,
            'binary': self._character_format_2,
            'bit': self._numeric_format_1,
            'blob': self._character_format_1,
            'char': self._character_format_2,
            'date': self._datetime_format_1,
            'datetime': self._datetime_format_2,
            'decimal': self._numeric_format_2,
            'double precision': self._numeric_format_1,
            'double': self._numeric_format_1,
            'enum': self._character_format_1,
            'float': self._numeric_format_1,
            'geometry': self._other_format_1,
            'geometrycollection': self._other_format_1,
            'int': self._numeric_format_1,
            'integer': self._numeric_format_1,
            'json': self._other_format_1,
            'linestring': self._other_format_1,
            'longblob': self._character_format_1,
            'longtext': self._character_format_1,
            'mediumblob': self._character_format_1,
            'mediumint': self._numeric_format_1,
            'mediumtext': self._character_format_1,
            'multipoint': self._other_format_1,
            'multipolygon': self._other_format_1,
            'multistring': self._other_format_1,
            'numeric': self._numeric_format_2,
            'point': self._other_format_1,
            'polygon': self._other_format_1,
            'real': self._numeric_format_1,
            'set': self._character_format_1,
            'smallint': self._numeric_format_1,
            'text': self._character_format_1,
            'time': self._datetime_format_2,
            'timestamp': self._datetime_format_2,
            'tinyblob': self._character_format_1,
            'tinyint': self._numeric_format_1,
            'tinytext': self._character_format_1,
            'varbinary': self._character_format_2,
            'varchar': self._character_format_2,
            'year': self._numeric_format_1,
        }

        format_data_type = data_types.get(self.data_type)

        if format_data_type is None:
            raise ex.SQLDataTypeError(f'{self.data_type} data type is not listed as a MySQL Data Type.')
        return format_data_type()

    def _other_format_1(self):
        return f"{self.data_type}"

    def __repr__(self):
        return f"{__class__.__name__}()"