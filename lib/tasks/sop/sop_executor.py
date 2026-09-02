"""
    The SOPExecutor class provides methods for executing and logging
    a sequence of Standard Operating Procedures (SOPs) defined in a
    JSON configuration file. It supports various data processing
    actions such as reading and writing DataFrames, executing SQL
    queries, schema comparison, and managing Glue crawlers.

    Key functionalities include:
    - Loading a configuration file from S3 or local storage.
    - Executing SQL queries in Spark or Athena with optional view creation.
    - Managing Glue crawlers to automate table creation and updates.
    - Handling data transformations, schema comparisons, and S3 file.

    :param spark: Spark session object used for executing Spark-specific
                  data operations.
    :param logger: Logger instance for logging activities and errors.
    :param config: Configuration object containing the path to the SOP
                   configuration file.
     "step1_run_crawler": {
      "action": "run_crawler",
      "database_name": "my_database",
      "table_name": "my_table",
      "s3_target_path": "s3://my-bucket/my-data-path/",
      "role": "my_custom_glue_role",
      "crawler_name": "my_custom_crawler_name"
    }
"""
import json
import re
import shlex
import subprocess
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from lib.tasks.glue.crawler import GlueCrawlerManager


class SOPExecutor:
    """
    This script defines the SOPExecutor class for executing
    and logging Standard Operating Procedures (SOPs) read from
    a configuration file.
    """

    def __init__(self, spark, logger, config):
        """
        Initialize with logger, Spark session, and config file.

        :param spark: Spark session object.
        :param logger: Logger instance for logging.
        :param config: Configuration object with config file path.
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.config_file = self.config.config_file
        self.glue_crawler_manager = GlueCrawlerManager(logger)
        self.prof_sql_queries = self.read_queries()
        self.last_select_df = None
        self.__validate_keys()

    def read_queries(self):
        """
        Read queries from the JSON configuration file.
        """
        if self.config_file.startswith('s3://'):
            config_data = self.read_from_s3(self.config_file)
        else:
            with open(self.config_file, 'r', encoding='utf-8') as file:
                config_data = json.load(file)
        return config_data['sql']

    def read_from_s3(self, s3_path):
        """
        Read a JSON configuration file from an S3 path.

        :param s3_path: Full S3 path of the configuration file
        (e.g., 's3://bucket-name/path/to/file.json').
        :return: Parsed JSON data from the configuration file.
        :raises NoCredentialsError: If AWS credentials are not available.
        :raises ClientError: If there is an error accessing the S3 object.
        """
        s3 = boto3.client('s3')
        bucket_name, key = self.split_s3_path(s3_path)
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=key)
            config_content = obj['Body'].read().decode('utf-8')
            config_data = json.loads(config_content)
            return config_data
        except NoCredentialsError:
            self.logger.error("Credentials not available for S3")
            raise
        except ClientError as e:
            self.logger.error(f"Failed to read configuration from S3: {e}")
            raise

    def split_s3_path(self, s3_path):  # noqa
        """
        Split an S3 path into its bucket and key components.

        :param s3_path: Full S3 path in the format 's3://bucket-name/key/path'.
        :return: Tuple containing the bucket name and key.
        :raises ValueError: If the S3 path is not in the correct format.
        """
        if s3_path.startswith("s3://"):
            path_parts = s3_path[len("s3://"):].split("/", 1)
            if len(path_parts) == 2:
                return path_parts[0], path_parts[1]
        raise ValueError(f"Invalid S3 path: {s3_path}")

    def __validate_keys(self):
        """
        Validate that all keys start with 'step' followed by a number.
        """
        for key in self.prof_sql_queries.keys():
            if not re.match(r'^step\d+_', key):
                self.logger.error(
                    f"Invalid step key: {key}. Keys should follow the "
                    f"format 'stepXXX_description' where 'XXX' is a "
                    f"numeric value."
                )
                raise ValueError(
                    f"Invalid step key: {key}. Keys should follow the "
                    f"format 'stepXXX_description' where 'XXX' is a "
                    f"numeric value."
                )

    def print_and_execute_queries(self):
        """
        Print the SQL queries using the logger and execute them.
        """
        sorted_keys = sorted(
            self.prof_sql_queries.keys(),
            key=lambda x: int(re.match(r'step(\d+)_', x).group(1))
        )
        for key in sorted_keys:
            query = self.prof_sql_queries[key]
            self.logger.info(f"Executing {key}")
            self.logger.info(
                "***********************************************************"
            )
            action, value = self.__parse_action_value(query)
            if action == "s3_copy":
                self.__execute_s3_copy(value)
            elif action == "read":
                self.__execute_read(value, query.get('view'))
            elif action == "write":
                if self.last_select_df is None:
                    self.logger.error(f"Missing select query before {key}")
                    raise ValueError(f"Missing select query before {key}")
                self.__execute_write(value, query.get('view'))
            elif action == "execute_query":
                self.__execute_query(value, query.get('view'),
                                     query.get('engine'))
            elif action == "compare_schema":
                self.__execute_compare_schema(value)
            elif action == "run_crawler":
                self.__execute_crawler(value)
            self.logger.info(f"Completed {key}: {query}")
            self.logger.info(
                "###########################################################"
            )

    def __parse_action_value(self, query): # noqa
        """
        Parse the action and value from the query dictionary.

        :param query: Query dictionary to parse.
        :return: Tuple of (action, value).
        """
        action = query.get('action')
        if action in ["write", "read", "compare_schema", "run_crawler"]:
            value = query
        else:
            value = query.get('path') or query.get('query')
        return action, value

    def __execute_crawler(self, command):
        """
        Execute a Glue crawler action using GlueCrawlerManager.

        :param command: Dictionary containing crawler configuration.
        """
        database_name = command.get("database_name")
        table_prefix = command.get("table_prefix", None)
        self.logger.info(f"table_prefix: {table_prefix}")
        s3_target_path = command.get("s3_target_path")
        role = command.get("role")
        crawler_name = command.get("crawler_name")
        if not (database_name and s3_target_path):
            self.logger.error("Crawler action missing required fields.")
            raise ValueError("Crawler action missing required fields.")

        self.logger.info(f"Starting crawler for database: {database_name}, "
                         f"table: {table_prefix} at path: {s3_target_path}")

        self.glue_crawler_manager.create_and_run_crawler(
            database_name=database_name,
            table_prefix=table_prefix if table_prefix else None,
            s3_target_path=s3_target_path,
            role=role,
            crawler_name=crawler_name
        )

    def __is_valid_s3_path(self, path):
        """
        Check if the provided path is a valid S3 path.

        :param path: Path to check.
        :return: True if the path is valid, else False.
        """
        return isinstance(path, str) and path.startswith("s3://")

    def __execute_read(self, command, view=None):
        """
        Execute a read command to load a DataFrame from an S3 path,
        transform column names to uppercase, and log its schema and count.

        :param command: Dictionary containing the read command details,
                        with the S3 path under the 'path' key.
        :param view: Optional view name for creating a temporary Spark
                     view from the DataFrame.
        :raises ValueError: If the S3 path provided is invalid.
        """
        path = command['path']
        if not self.__is_valid_s3_path(path):
            self.logger.error(f"Invalid S3 path: {path}")
            raise ValueError(f"Invalid S3 path: {path}")
        df = self.spark.read.parquet(path)
        df = self.__transform_column_names_to_upper(df)
        self.logger.info("Schema:")
        df.printSchema()
        self.logger.info(f"Count: {df.count()}")
        if view:
            df.createOrReplaceTempView(view)
            self.logger.info(f"Created temporary view: {view}")
        self.last_select_df = df
        self.log_schema_and_count(df, path)

    def log_schema_and_count(self, df, path):
        """
        Log the schema and record count of a DataFrame.

        :param df: The DataFrame to log.
        :param path: The path associated with the DataFrame.
        """
        self.logger.info(f"\n{'*' * 70}")
        self.logger.info(f"Schema and Record Count for Path: {path}")
        self.logger.info(f"Record Count: {df.count()}")
        self.logger.info("Schema:")
        df.printSchema()
        self.logger.info(f"{'*' * 70}\n")

    def __execute_write(self, command, view=None):
        """
        Execute a write command to save a DataFrame to a specified
        S3 path with optional partitioning and maxRecordsPerFile settings.

        :param command: Dictionary containing the write command details,
                        including 'path' (S3 path to save the DataFrame),
                        optional 'partition_fields' (comma-separated string
                        of fields for partitioning), and 'max_records_per_file'
                        (maximum records per output file).
        :param view: Optional view name from which to read the data before
                     writing. If provided, data is selected from this view.
        :raises ValueError: If the S3 path is invalid or if required data
                            is missing prior to writing.
        """
        path = command['path']
        partition_fields = command.get('partition_fields')
        max_records_per_file = command.get('max_records_per_file', 1000000)

        if not self.__is_valid_s3_path(path):
            self.logger.error(f"Invalid S3 path: {path}")
            raise ValueError(f"Invalid S3 path: {path}")

        if view:
            self.last_select_df = self.spark.sql(f"SELECT * FROM {view}")

        writer = self.last_select_df.write.mode('overwrite') \
            .option("maxRecordsPerFile", max_records_per_file)

        if partition_fields:
            writer = writer.partitionBy(*partition_fields.split(","))

        writer.parquet(path)
        self.logger.info(f"Data written to {path}")
        self.__log_schema_and_count(path, "Write")

    def __execute_compare_schema(self, command):
        """
        Execute a compare_schema action to compare the schemas of
        two Parquet files stored in S3.

        :param command: Dictionary containing the paths for the two
                        Parquet files under 'path1' and 'path2' keys.
        :raises ValueError: If the paths for either Parquet file are
                            missing or invalid.
        """
        path1 = command['path1']
        path2 = command['path2']
        self.logger.info(f"Comparing schemas between {path1} and {path2}...")
        self.compare_schemas(path1, path2)

    def __execute_s3_copy(self, command):
        """
        Execute an S3 sync or copy command to copy files and subfolders from a source S3
        path to a target S3 path, maintaining the folder hierarchy.

        :param command: String containing the S3 command, where the
                        source and target S3 paths are the last two elements.
                        Example format: 'aws s3 cp s3://source/path s3://target/path/'.
        :raises ValueError: If the command fails or no files are copied.
        """
        if "aws s3 cp" in command:
            self.logger.info("Replacing 'aws s3 cp' with 'aws s3 sync' for recursive copy.")
            command = command.replace("aws s3 cp", "aws s3 sync")

        try:
            self.logger.info(f"Executing command: {command}")
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                self.logger.error(f"""Error during S3 operation:
                                      {result.stderr}""")
                raise ValueError(f"Failed to execute command: {result.stderr}")
            self.logger.info(f"Successfully executed S3 command: {command}")

        except Exception as e:
            self.logger.error(f"""Failed to execute command:
                                  {command}, Error: {e}""")
            raise

    def __log_schema_and_count(self, path, label):
        """
        Log the schema and record count of a DataFrame read from the specified
        S3 path.
        :param path: S3 path to the Parquet file where the DataFrame
        is located.
        :param label: Label used in the log output to identify the DataFrame
                      (e.g., "Source" or "Target").
        :raises ValueError: If the path is invalid or if the file cannot be
        read.
        """
        df = self.spark.read.parquet(path)
        df = self.__transform_column_names_to_upper(df)
        self.logger.info(f"\n{'*' * 70}")
        self.logger.info(f"{label} Schema and Count:")
        df.printSchema()
        self.logger.info(f"{label} Count: {df.count()}")
        self.logger.info(f"{'*' * 70} \n")

    def compare_schemas(self, path1, path2):
        """
        Compare the schemas of two Parquet files located at the specified
        S3 paths.
        :param path1: S3 path to the first Parquet file.
        :param path2: S3 path to the second Parquet file.
        :raises ValueError: If either path is invalid or if there is an issue
                            reading the files.
        :logs: Logs the schema comparison results. Logs if the schemas are
               identical or different.
        """
        df1 = self.spark.read.parquet(path1)
        df2 = self.spark.read.parquet(path2)
        schema1 = df1.schema
        schema2 = df2.schema
        self.log_schema_and_count(df1, path1)
        self.log_schema_and_count(df2, path2)
        if schema1 == schema2:
            self.logger.info("Schema of both files is identical.")
        else:
            self.logger.warn("Schema of both files is different.")

    def __execute_query(self, query, view=None, engine="spark"):
        """
        Execute an SQL query. If it's a CREATE statement, the specified
        engine (either 'spark' or 'athena') is used; otherwise, the query
        is executed directly in Spark.

        :param query: SQL query string to execute.
        :param view: Optional name for creating a temporary view for SELECT
                     queries. If provided, the result is saved as a temporary
                     view with this name.
        :param engine: Specifies the engine for executing CREATE statements.
                       Valid values are 'spark' (default) or 'athena'.
        :raises ValueError: If an unsupported engine is specified.
        :raises Exception: If the query execution fails.
        :logs: Logs the schema and record count for SELECT queries. Logs
               completion or errors for CREATE statements.
        """
        if query.strip().upper().startswith("CREATE"):
            if engine == "spark":
                self.__execute_spark_query(query)
            else:
                raise ValueError(f"Unsupported engine: {engine}")
        else:
            try:
                df = self.spark.sql(query)
                df = self.__transform_column_names_to_upper(df)
                self.logger.info("Schema:")
                df.printSchema()
                self.logger.info(f"Count: {df.count()}")
                if view:
                    df.createOrReplaceTempView(view)
                    self.logger.info(f"Created temporary view: {view}")
                self.last_select_df = df
            except Exception as e:
                self.logger.error(f"""Failed to execute query:
                                  {query}, Error: {e}""")
                raise

    def __read_transform_write_back_to_s3(self, path):
        """
        Read Parquet files from an S3 path, transform column names to
        uppercase,
        and write the transformed DataFrame back to the same S3 path.

        :param path: S3 path of the Parquet files to read, transform, and
        overwrite.
        :raises Exception: If there is an error reading, transforming, or
        writing
                           back the Parquet files.
        :logs: Logs the schema of the transformed DataFrame and confirms
               successful write-back to the specified S3 path.
        """
        try:
            df = self.spark.read.parquet(path)
            df = self.__transform_column_names_to_upper(df)
            df.write.mode('overwrite').parquet(path)
            self.logger.info(f"""Transformed and wrote
                             back Parquet files to {path}""")
        except Exception as e:
            self.logger.error(f"""Failed to read/transform/write
                              back to S3 path: {path}""")
            self.logger.error(f"Error details: {str(e)}")
            raise

    def __delete_files_in_s3_path(self, path):
        # pylint:disable=unused-private-member
        """
        Delete all non-Parquet files in the specified S3 path.

        :param path: S3 path where files will be checked and non-Parquet
                     files deleted.
        :raises ClientError: If there is an error accessing or deleting files
                             in the S3 path.
        :logs: Logs the start of the deletion process, lists each deleted
               file, and confirms if no non-Parquet files were found.
        """
        bucket_name, prefix = self.split_s3_path(path)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        self.logger.info(f"Deleting non-Parquet files in S3 path: {path}")
        try:
            objects_to_delete = [
                {'Key': obj.key}
                for obj in bucket.objects.filter(Prefix=prefix)
                if not obj.key.endswith('.parquet')
            ]
            if objects_to_delete:
                bucket.delete_objects(Delete={'Objects': objects_to_delete})
                self.logger.info(f"""Deleted non-Parquet files in S3 path:
                                     {path}""")
            else:
                self.logger.info(f"""No non-Parquet files found in S3 path:
                                     {path}""")
        except ClientError as e:
            self.logger.error(f"Failed to delete files in S3 path {path}: {e}")
            raise

    def __execute_spark_query(self, query):
        """
        Execute a CREATE SQL query using Spark SQL, optionally handling
        external locations by transforming the query.

        :param query: SQL query string to execute, typically a CREATE TABLE
                      statement that may specify an external location.
        :raises Exception: If the query execution fails.
        :logs: Logs the query execution process, schema transformation
               (if applicable), and confirms successful execution.
        """
        self.logger.info(f"Executing Spark SQL: {query}")
        try:
            external_location = re.search(r"external_location\s*=\s*'([^']+)'",
                                          query, re.IGNORECASE)
            if external_location:
                external_path = external_location.group(1)
                query = re.sub(r"external_location\s*=\s*'([^']+)'",
                               f"LOCATION '{external_path}'", query,
                               flags=re.IGNORECASE)
            self.spark.sql(query)
            self.logger.info("Spark SQL query executed successfully")
            if external_location:
                self.__read_transform_write_back_to_s3(external_path)
        except Exception as e:
            self.logger.error(f"Failed to execute Spark SQL query: {query}")
            self.logger.error(f"Error details: {str(e)}")
            raise

    def __transform_column_names_to_upper(self, df):  # noqa
        """
        Transform all column names in a Spark DataFrame to uppercase.

        :param df: Spark DataFrame whose column names will be transformed.
        :return: A new DataFrame with all column names in uppercase.
        """
        for col in df.columns:
            df = df.withColumnRenamed(col, col.upper())
        return df

    def __transform_query_columns_to_upper(self, query):
        # pylint:disable=unused-private-member
        """
        Transform column names in an SQL query to uppercase.

        :param query: SQL query string in which column names should be
                      transformed to uppercase.
        :return: Modified SQL query string with all column names in uppercase.
        """
        return re.sub(r'\"(\w+)\"',
                      lambda match: f'"{match.group(1).upper()}"',
                      query
                      )
