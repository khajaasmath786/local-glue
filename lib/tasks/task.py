"""

This module provides a TaskExecutor class for handling database operations.
The TaskExecutor class simplifies database operations by providing a simple
interface for connecting to a database, executing queries, and retrieving
column information from query results.

The class uses configuration settings to establish a database connection and
execute SQL queries. It also allows for custom queries to be passed as
arguments or uses a predefined query configured in the class.

Example usage:

    from task_executor import TaskExecutor

    # Initialize a TaskExecutor instance with a configuration dictionary.
    executor = TaskExecutor(config)

    # Execute a query using the predefined or custom SQL query.
    executor.execute_query()

    # Retrieve column information from the query result.
    columns = executor.get_column_info(query_result)

For more information, see the docstrings for the TaskExecutor class and its 
methods.
"""
import logging
import os

import cx_Oracle

from lib.common.load_config import ConfigHelper
from lib.db.mysql_connector import MySQLConnector
from lib.db.oracle_connector import OracleDBConnector
from lib.db.postgress_connector import PostgreSQLConnector
from lib.tasks.data_generator.json import JSONGenerator
from lib.tasks.data_generator.sql import SqlGenerator
from lib.utils.encrypt import PasswordManager
from lib.utils.util import FileOperations

DEFAULT_DATABASE_TYPE = 'oracle'
DEFAULT_HOST = 'host'
DEFAULT_USERNAME = 'username'
DEFAULT_PASSWORD = 'password'
DEFAULT_KEY = 'key'
DEFAULT_SERVICE_NAME = 'service_name'


class TaskExecutor:
    """
    This class represents a task executor for handling database operations.

    """

    def __init__(self, config):
        """
        Initialize a TaskExecutor instance.

        :param config: A configuration dictionary containing various settings.
        :type config: dict
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.schema = ConfigHelper.get_config(config, "database", "schema")
        self.table = ConfigHelper.get_config(config, "database", "table")
        self.connection = self._get_db_connection()
        self.query = self._get_query()
        self.ks_query = self._get_ks_streams_query()

    def log_error(self, e):

        if e.args:
            error, = e.args
            self.logger.info(f"Oracle error code: {error.code}")
            self.logger.info(f"Oracle error message: {error.message}")
            self.logger.info(f"Oracle error context: {error.context}")
            self.logger.info(f"Oracle error offset: {error.offset}")
            self.logger.info(f"Oracle error position: {error.position}")
            self.logger.info(f"Oracle error statement: {error.statement}")

    def _get_db_connection(self):
        """
        Get a database connection based on the configuration settings.

        :return: A database connection object.
        :rtype: cx_Oracle.Connection
        """
        database_type = ConfigHelper.get_config(self.config, "database", "database_type", default=DEFAULT_DATABASE_TYPE)
        host = ConfigHelper.get_config(self.config, "database", "host", default=DEFAULT_HOST)
        username = ConfigHelper.get_config(self.config, "database", "username", default=DEFAULT_USERNAME)
        password = ConfigHelper.get_config(self.config, "database", "password", default=DEFAULT_PASSWORD)
        key = ConfigHelper.get_config(self.config, "database", "key", default=DEFAULT_KEY)
        password = PasswordManager().decrypt_password(key, password)
        database_name = ConfigHelper.get_config(self.config, "database", "service_name", default=DEFAULT_SERVICE_NAME)
        schema = ConfigHelper.get_config(self.config, "database", "schema")
        table = ConfigHelper.get_config(self.config, "database", "table")

        #query = FileOperations.read_sql_query(schema, table)

        self.logger.info(f"Database Type: {database_type}")

        self.logger.info(f"Host: {host}")
        self.logger.info(f"Username: {username}")
        self.logger.info(f"password: {password}")
        self.logger.info("Password: ********")  # Don't log the password
        self.logger.info(f"Service Name: {database_name}")
        #self.logger.info(f"SQL Query: {query}")

        # Create a DB connector instance based on the database type
        db_connector = None
        if database_type == "oracle":
            db_connector = OracleDBConnector(username, password, host,
                                             database_name, port="1521")
        elif database_type == "mysql":
            db_connector = MySQLConnector(username, password, host, database_name, port="1521")
        elif database_type == "postgresql":
            db_connector = PostgreSQLConnector(username, password, host, database_name, port="1521")
        else:
            self.logger.info("Unsupported database type")

        # Establish the database connection
        self.logger.info("Before connecting the DB")
        connection = db_connector.connect()
        self.logger.info("After connecting the DB")

        return connection

    def _get_query(self):
        """
        Get the SQL query based on the schema and table configuration.

        :return: The SQL query as a string.
        :rtype: str
        """
        schema = ConfigHelper.get_config(self.config, "database", "schema")
        table = ConfigHelper.get_config(self.config, "database", "table")
        print(table)
        return FileOperations.read_sql_query(schema=schema, table=table,filename='read_db.sql')

    def _get_ks_streams_query(self):
        """
        Get the SQL query based on the schema and table configuration.

        :return: The SQL query as a string.
        :rtype: str
        """
        schema = ConfigHelper.get_config(self.config, "database", "schema")
        table = ConfigHelper.get_config(self.config, "database", "table")
        return FileOperations.read_sql_query(schema, table,'pumps.sql')

    def create_file(self, text, file_type, filename=None, output_path=None, file_extension="txt"):
        """
        Save text to a file with a custom name and output path.

        :param text: Text data to save.
        :type text: str
        :param file_type: Type of the file ("schema" or "sql").
        :type file_type: str
        :param filename: Custom filename for the output file (without extension).
        :type filename: str
        :param output_path: Custom output path. If None, the current directory is used.
        :type output_path: str
        :param file_extension: File extension to use for the output file (e.g., "txt" or "json").
        :type file_extension: str
        """

        if output_path is None:
            if file_type == "schema":
                output_directory = 'schema'
            elif file_type == "sql":
                output_directory = 'sql'
            else:
                raise ValueError("Invalid file_type. Use 'schema' or 'sql'.")

            script_directory = os.path.dirname(os.path.abspath(__file__))
            output_path = os.path.join(script_directory, os.pardir, os.pardir, 'configs', 'data_stream',
                                       output_directory)

        file_name = os.path.join(output_path, f"{filename}.{file_extension}")
        with open(file_name, 'w') as text_file:
            text_file.write(text)
        self.logger.info(f"Saved text to {file_name}")
        


    def generate_sql_schema_files(self, query=None,ks_streams_query=None,filename=None):

        """
        Execute a database query based on the configuration settings.
        """
        if filename is None:
            filename = f"{self.schema}_{self.table}".upper()
        if query is None:
            query = self.query
            self.create_schema_files(filename,query)
        if ks_streams_query is None:
            ks_streams_query = self.ks_query
            self.create_sql_files(filename,ks_streams_query)

    def create_schema_files(self, filename=None,query=None):
        """
        Execute a database query based on the configuration settings.
        """
        if query is None:
            query = self.query
        try:
            if self.connection:
                self.logger.info("Connected to DB")
                json_generator = JSONGenerator(self.connection)
                # query_sql = ConfigHelper.get_config(self.config, "database", "query", default=DEFAULT_SERVICE_NAME)
                cursor = self.connection.cursor()
                cursor.execute(f"{query}")
                query_result = json_generator.generate_json_from_cursor(cursor)
                self.logger.info(query_result)
                self.create_file(text=query_result, file_type='schema', filename=filename, file_extension="schema")
                cursor.close()
        except cx_Oracle.DatabaseError as e:
            self.log_error(e)

    def create_sql_files(self, filename=None,query=None):
        """
        Execute a database query based on the configuration settings.
        """
        if query is None:
            query = self.query
        try:
            if self.connection:
                self.logger.info("Connected to DB")
                sql_generator = SqlGenerator(self.connection)
                cursor = self.connection.cursor()
                cursor.execute(f"{query}")
                query_result = sql_generator.generate_sql_from_cursor(cursor,
                                                                      schema_name=filename.split("_")[0].upper(),
                                                                      table_name=filename.split("_")[1].upper())
                self.logger.info(query_result)
                self.create_file(text=query_result, file_type='sql', filename=filename,file_extension="sql")
                cursor.close()
        except cx_Oracle.DatabaseError as e:
            self.log_error(e)


    def get_column_info(self, result):
        """
        Get column information from the query result.

        :param result: The query result.
        :type result: Any

        :return: A list of column names.
        :rtype: list
        """
        self.logger.info("Retrieving column information from the query result")
        return result.keys()
