import json
from lib.common.constants import Constants
from lib.common.constants import ConstantFields
import sqlparse
class SqlGenerator:
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
        result = {}
        for field in ConstantFields:
            result.update(field.value)
            json_output[Constants.RECORD_COLUMNS.value].append(result)
        return json_output

    def generate_column_definitions(self, rows):
        # Generate column definitions for the CREATE STREAM statement
        destination_sql_stream = ''
        for row in rows:
            column_name, data_type, data_precision = row[0], row[1], row[2]

            # Get SQL type
            sql_type = "DECIMAL(19)" if data_type == "NUMBER" else f"VARCHAR({data_precision})"

            # Add the column definition to the SQL statement
            destination_sql_stream += f'"{column_name.lower()}" {sql_type.lower()},\n'

        return destination_sql_stream

    def generate_destination_sql(self, table_name, column_definitions):
        # Generate the destination SQL statement
        print(column_definitions)

        # Initialize the SQL statement
        columns = [
            ("op", "varchar(255)"),
            ("commit_timestamp", "varchar(255)"),
            ("change_seq", "varchar(255)"),
            ("h_user", "varchar(255)")
        ]

        # Create the SQL statement using an f-string
        destination_sql_stream = f'CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM_{table_name}" \n(\n'
        for column_name, data_type in columns:
            destination_sql_stream += f'"{column_name}" {data_type}, \n'
        destination_sql_stream += column_definitions
        destination_sql_stream = destination_sql_stream.rstrip('\n, ')
        destination_sql_stream += ");"
        return destination_sql_stream

    def generate_pump_sql(self, table_name, schema_name, rows):
        # Generate the pump SQL statement
        pump_sql_stream = (
            f'CREATE OR REPLACE PUMP "STREAM_PUMP_{table_name}" AS\n'
            f'INSERT INTO "DESTINATION_SQL_STREAM_{table_name}"\n'
            'SELECT STREAM case\n'
            '    when "operation"=\'load\' then \' \'\n'
            '    when "operation"=\'insert\' then \'I\'\n'
            '    when "operation"=\'update\' then \'U\'\n'
            '    when "operation"=\'delete\' then \'D\'\n'
            'end as op,\n'
            '"COL_timestamp" as commit_timestamp,\n'
            '"change_seq",\n'
            '"h_user" as h_user,\n'
        )

        for row in rows:
            column_name, data_type, data_precision = row[0], row[1], row[2]
            pump_sql_stream += f'{column_name.lower()}, \n'

        pump_sql_stream = pump_sql_stream.rstrip('\n, ')
        pump_sql_stream += (
            f'\nFROM "SOURCE_SQL_STREAM_001"\n'
            f'WHERE "COL_tablename"=\'{table_name}\'\n'
            f'and "COL_schemaname"=\'{schema_name}\';'
        )

        return pump_sql_stream

    def generate_delete_pump_sql(self, table_name, schema_name, rows):
        # Generate the pump SQL statement
        delete_sql_stream = (
            f'CREATE OR REPLACE PUMP "STREAM_PUMP_{table_name}_D" AS\n'
            f'INSERT INTO "DESTINATION_SQL_STREAM_{table_name}"\n'
            'SELECT STREAM \'D\' as op,\n'
            '"COL_timestamp" as commit_timestamp,\n'
            '"change_seq",\n'
            '"h_user" as h_user,\n'
        )
        where_conditions = []
        primary_key_conditions = []
        for row in rows:
            column_name, data_type, data_precision, is_primary_key = row[0], row[1], row[2], row[3]
            if is_primary_key == 'Y':
                delete_sql_stream += f'"bi_{column_name.lower()}" as {column_name.lower()}, \n'
                primary_key_conditions.append(f'nullif("bi_{column_name.lower()}", {column_name.lower()}) is not null')
                #where_conditions.append(f'nullif("bi_{column_name}", {column_name}) is not null')
            else:
                delete_sql_stream += f'{column_name.lower()},\n'

        delete_sql_stream = delete_sql_stream.rstrip('\n, ')
        delete_sql_stream += (
            f'\nFROM "SOURCE_SQL_STREAM_001"\n'
            f'WHERE "COL_tablename"=\'{table_name}\'\n'
            f'and "COL_schemaname"=\'{schema_name}\'\n'
            f'and "operation"=\'update\' '
        )
        if primary_key_conditions:
            where_conditions.append('(' + ' or '.join(primary_key_conditions) + ')')
        if where_conditions:
            delete_sql_stream += f'and {", ".join(where_conditions)}'
        delete_sql_stream += ';'
        return delete_sql_stream

    def generate_sql_from_cursor(self,cursor, schema_name="table_name", table_name="table_name"):
        rows = cursor.fetchall()
        if not rows:
            return "No rows found"

        column_definitions = self.generate_column_definitions(rows)
        destination_sql = self.generate_destination_sql(table_name,column_definitions)
        # Generate pump SQL statement
        pump_sql = self.generate_pump_sql(table_name, schema_name, rows)
        delete_pump_sql=self.generate_delete_pump_sql(table_name, schema_name, rows)


        # Format each SQL statement
        sql_statements_string=destination_sql + "\n" + pump_sql + "\n" + delete_pump_sql
        sql_statements = [statement.strip() for statement in sql_statements_string.split(';') if statement.strip()]
        formatted_sql = "\n\n".join(
            [sqlparse.format(statement, reindent=True, keyword_case='upper') + ';' for statement in sql_statements])

        return sql_statements_string




