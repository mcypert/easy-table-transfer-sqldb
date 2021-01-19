import pyodbc
import helperfunctions as hf
import sqldbconnect.connectexcept as ex


class SQLSoftwareConnect:
    """Class that uses pyodbc to connect to a specific SQL software system and switch between SQL connections on the
    fly.
    """

    def __init__(self, driver, server, uid, pwd, database, port=None, charset=None):
        """
        :param str driver: The specified driver that connects to the SQL software system.  Note:  The driver must be
            installed on the device -> check pyodbc documentation to see what drivers are installed.
        :param str server: The specified server that is being connected to within the SQL software system.
        :param str uid: The UserID used to gain access and connect to the SQL software system.
        :param str pwd: The Password used to gain access and connect to the SQL software system.
        :param str database: The specified database within the SQL software system.
        :param str/None port: The specified SQL software system port.
        :param str/None charset: The specified SQL software system charset.
        """
        self.driver = driver
        self.server = server
        self.uid = uid
        self.pwd = pwd
        self.database = database
        self.port = port
        self.charset = charset

    def sql_software_connect(self):
        """Connects to a specific SQL software system.  Warning: Autocommit is set to True - meaning the database
        executes a commit automatically after every SQL statement, so transaction management by the client is not
        possible.

        :return: The Cursor of a specific SQL software system, Exception is raised if connection was not successful.
        :rtype: pyodbc Cursor Object
        """
        connection_string = self._build_connection_string()
        cnx = pyodbc.connect(connection_string, autocommit=True)
        cursor = cnx.cursor()
        return cursor

    def change_multiple_variables(self, variables):
        """Allows the user to switch driver, server, uid, pwd, database, port, charset variables on an already specified
        SQLSoftwareConnect instance.  Important:  If this method is used to change a connection variable, the user will
        have to reconnect to a specified SQL software system using sql_software_connect with the updated connection
        variable.

        :param dict variables: dictionary with at least one of the following as driver, server, uid, pwd, database,
            port, charset dictionary keys and the specified user input as the dictionary values.  For example, if the
            user wants to change the database then variables = {'database': <change database here>}
        """
        for k, v in variables.items():
            if k.upper() not in ['DRIVER', 'SERVER', 'UID', 'PWD', 'DATABASE', 'PORT', 'CHARSET']:
                raise ex.SQLConnectionVariableError("variables needs to have: "
                                                    "'DRIVER', 'SERVER', 'UID', 'PWD', 'DATABASE' dict keys")
            else:
                k = k.lower()
                self.__dict__[k] = v

    def _build_connection_string(self):
        """Builds a pyodbc connection string from the instance variables.

        :return: A pyodbc connection string
        :rtype: str
        """
        connection_string = ''
        for k, v in self.__dict__.items():
            if v is not None:
                connection_string += k.upper() + "=" + str(v) + ";"
        return connection_string

    def __repr__(self):
        return f'{__class__.__name__}({hf.add_quotes(self.driver)}, ' \
               f'{hf.add_quotes(self.server)}, ' \
               f'{hf.add_quotes(self.uid)}, ' \
               f'{hf.add_quotes(self.pwd)}, ' \
               f'{hf.add_quotes(self.database)}, ' \
               f'{hf.add_quotes(self.port)}, ' \
               f'{hf.add_quotes(self.charset)})'