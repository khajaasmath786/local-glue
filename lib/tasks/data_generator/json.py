import json
import sys

from lib.common.constants import Constants
from lib.common.constants import ConstantFields
class JSONGenerator:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def get_column_descriptions(self, cursor):
        return cursor.description

    def is_bi_column(self, column_name):
        return column_name.startswith(Constants.BI_PREFIX.value)

    def get_sql_type(self, is_bi_column):
        return Constants.VARCHAR_TYPE.value if is_bi_column else Constants.DECIMAL_TYPE.value

    def get_mapping(self, column_name, is_bi_column):
        if is_bi_column:
            return Constants.BEFORE_IMAGE_PREFIX.value+ column_name.upper()
        else:
            return Constants.DATA_PREFIX.value + column_name

    def create_json(self,json_output):

        for column_name,mapping in Constants.FIELDS.value.items():
            row_data = {
                Constants.NAME.value: column_name,
                Constants.MAPPING.value: mapping,
                Constants.SQL_TYPE.value: Constants.VARCHAR_TYPE_256.value
            }
            json_output[Constants.RECORD_COLUMNS.value].append(row_data)

        return json_output

    def generate_json_from_cursor(self, cursor):
        # Get column descriptions
        column_descriptions = self.get_column_descriptions(cursor)
        rows=cursor.fetchall()

        # Initialize the JSON structure
        json_output = {
            Constants.RECORD_COLUMNS.value: []
        }
        print(rows)

        # Loop through the rows in the cursor
        for row in rows:

            column_name, data_type, data_precision = row[0], row[1], row[2]

            # Determine if the column name starts with "bi_"
            is_bi = column_name.startswith(Constants.BI_PREFIX.value)

            # Get SQL type and mapping
            sql_type = "DECIMAL" if data_type == "NUMBER" else f"VARCHAR({data_precision})"
            mapping = self.get_mapping(column_name, is_bi)

            # Create a dictionary for the current row's data
            row_data = {
                Constants.NAME.value: column_name,
                Constants.MAPPING.value: mapping,
                Constants.SQL_TYPE.value: sql_type
            }
            json_output[Constants.RECORD_COLUMNS.value].append(row_data)
        # Convert the JSON structure to a JSON string

        json_output = self.create_json(json_output)
        json_string = json.dumps(json_output, indent=4)
        return json_string

