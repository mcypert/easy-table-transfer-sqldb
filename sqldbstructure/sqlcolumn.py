from abc import ABC, abstractmethod
from sqldbstructure import sqldatatype
import sqldbconversion.sqldataconvert as sqlconvert

class SQLColumn(ABC):

    def __init__(self, column, char_size, num_precision, num_scale, date_precision, nullable, charset):
        self.column = column
        self.data_type = None
        self.char_size = char_size
        self.num_precision = num_precision
        self.num_scale = num_scale
        self.date_precision = date_precision
        self.nullable = nullable
        self.charset = charset

    def convert_column(self, software):
        """
        """
        convert_object = getattr(sqlconvert, software.replace('Engine', 'Conversion').replace('(', '').replace(')', ''))
        convert_object = convert_object(self.data_type.data_type, self.char_size, self.num_precision, self.num_scale, self.date_precision, self.charset)
        getattr(convert_object, f"{str(self).lower().replace('column()', '_conversion')}")()
        convert_object.column = self.column
        convert_object.nullable = self.nullable
        return convert_object

    def format_column(self):
        """
        """
        return f"{self.column} {self.data_type.data_type_format()} {self._format_nullable()}"

    def _format_nullable(self):
        """
        """
        if self.nullable.upper() == 'YES':
            return 'null'
        else:
            return 'not null'

    @abstractmethod
    def __repr__(self):
        pass

class SQLServerColumn(SQLColumn):

    def __init__(self, column, data_type, char_size, num_precision, num_scale, date_precision, nullable, charset):
        super().__init__(column, char_size, num_precision, num_scale, date_precision, nullable, charset)
        self.data_type = sqldatatype.SQLServerDataType(data_type, char_size, num_precision, num_scale, date_precision)

    def __repr__(self):
        return f"{__class__.__name__}()"

class MySQLColumn(SQLColumn):

    def __init__(self, column, data_type, char_size, num_precision, num_scale, date_precision, nullable, charset):
        super().__init__(column, char_size, num_precision, num_scale, date_precision, nullable, charset)
        self.data_type = sqldatatype.MySQLDataType(data_type, char_size, num_precision, num_scale, date_precision)

    def __repr__(self):
        return f"{__class__.__name__}()"