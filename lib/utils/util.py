from datetime import datetime
import os
import logging

from datetime import date, datetime
import os
import json


class FileOperations:

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def read_sql_query(schema, table, filename):
        # Determine the path to the SQL file
        script_directory = os.path.dirname(os.path.abspath(__file__))
        print('hai')
        print(filename)
        sql_file_path = os.path.join(script_directory, os.pardir, os.pardir, 'configs', 'sql', filename)
        with open(sql_file_path, 'r') as sql_file:
            sql_query = sql_file.read()
            sql_query = sql_query.replace('<SCHEMA>', schema)
            sql_query = sql_query.replace('<TABLE>', table)
        return sql_query

    @staticmethod
    def create_file(text, filename=None, output_path=None, file_extension="txt", logger=None):
        """
        Save text to a file with a custom name and output path.

        :param text: Text data to save.
        :type text: str
        :param filename: Custom filename for the output file (without extension).
        :type filename: str
        :param output_path: Custom output path. If None, the current directory is used.
        :type output_path: str
        :param file_extension: File extension to use for the output file (e.g., "txt" or "json").
        :type file_extension: str
        :param logger: Logger to use for logging (optional).
        :type logger: logging.Logger
        """
        if filename is None:
            current_time = datetime.now().strftime("%Y%m%d%H%M%S")  # Get current time as a formatted string
            filename = f"result_{current_time}"

        if output_path is None:
                output_path = os.getcwd()  # Use the current directory

        file_name = os.path.join(output_path, f"{filename}.{file_extension}")
        with open(file_name, 'w') as text_file:
            text_file.write(text)
        if logger:
            logger.info(f"Saved text to {file_name}")