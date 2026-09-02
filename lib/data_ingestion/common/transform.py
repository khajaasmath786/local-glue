"""
    Class for transforming data using configurations.
"""
from typing import Tuple, List
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.functions import input_file_name, when, concat_ws
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType, DecimalType, FloatType, StringType, BooleanType
from lib.data_ingestion.utils.parse_config import ConfigParser
from lib.common.logger import Logger
from lib.data_ingestion.common.file_processor import BatchFileProcessor
import sys

class DataTransformer:
    """
    Class for transforming data using configurations.
    """

    def __init__(self, s3_bucket: str,
                 config_path: str, s3_target_bucket: str):
        """
        Initializes the DataTransformer with S3 bucket and config path.
        :param s3_bucket: Source S3 bucket path.
        :param config_path: Configuration file path.
        :param s3_target_bucket: Target S3 bucket path.
        """
        self.config_path = config_path
        self.s3_bucket = s3_bucket
        self.s3_target_bucket = s3_target_bucket
        self.config_parser = ConfigParser(
            self.s3_bucket, self.config_path
        )
        self.config = self.config_parser.read_yaml_config()
        self.warehouse_path = f"s3://{s3_target_bucket.rstrip('/')}/" \
                              "GENSIGHT/global/"
        self.spark = self.get_spark_session(self.warehouse_path)
        self.logger = Logger(self.__class__.__name__)
        self.yaml_config = self.read_config(self.config)

    def read_config(self, config_path: str) -> dict:
        """
        Reads the YAML configuration from S3.
        :param config_path: Path to the configuration file.
        :return: Parsed YAML configuration.
        """
        try:
            return self.config_parser.read_yaml_config()
        except Exception as e:
            self.logger.error(f"Failed to read config from {config_path}: {e}")
            raise

    def get_spark_session1(self) -> SparkSession:
        """
        Creates and returns a Spark session.
        :return: SparkSession object.
        """
        return SparkSession.builder.appName("GlueApp").getOrCreate()
    
    def add_missing_columns_to_source(self, df: DataFrame, table: str) -> DataFrame:
        """
        Adds missing columns from the target table to the source DataFrame with default values.

        :param df: Source DataFrame.
        :param table: Target table name including catalog and database.
        :return: Updated DataFrame with missing columns added.
        """
        try:
            target_schema = self.spark.table(table).schema
            for field in target_schema.fields:
                if field.name not in df.columns:
                    df = df.withColumn(field.name, lit(None).cast(field.dataType))
                    self.logger.info(f"Added missing column '{field.name}' to source DataFrame with default value.")
            return df
        except Exception as e:
            self.logger.error(f"Failed to add missing columns to source DataFrame: {e}")
            raise

    def read_file(self, source_path: str, header: str,
                  delimiter: str) -> DataFrame:
        """
        Reads a CSV file into a DataFrame.
        :param source_path: Path to the source file.
        :param header: Whether the CSV file has a header row.
        :param delimiter: Delimiter used in the CSV file.
        :return: DataFrame with the CSV data.
        """
        try:
            df = self.spark.read.option("header", header).option(
                "sep", delimiter
            ).option(
                "quote", "\""
            ).option(
                "multiLine", "true"
            ).option(
                "escape", "\""
            ).csv(source_path)
            return df
        except Exception as e:
            self.logger.error(f"Failed to read file {source_path}: {e}")
            raise

    def write_file(self, df: DataFrame, target_path: str) -> None:
        """
        Writes a DataFrame to a CSV file.
        :param df: DataFrame to be written.
        :param target_path: Target path to write the CSV.
        """
        try:
            df.write.mode("overwrite").csv(target_path, header=True)
            self.logger.info(f"Data written to {target_path}")
        except Exception as e:
            self.logger.error(f"Failed to write file to {target_path}: {e}")
            raise

    def rename_columns(self, df: DataFrame, schema_mapping: dict) -> DataFrame:
        """
        Renames columns in a DataFrame based on schema mapping.
        :param df: Source DataFrame.
        :param schema_mapping: Dict mapping source columns to target columns.
        :return: DataFrame with renamed columns.
        """
        try:
            if schema_mapping:
                for source_col, target_col in schema_mapping.items():
                    df = df.withColumnRenamed(
                        source_col, target_col["bax_standard"]
                    )
            return df
        except Exception as e:
            self.logger.error(f"Error renaming columns: {e}")
            raise

    def apply_transformations(self, df: DataFrame,
                              transformations_needed: List[str]) -> DataFrame:
        """
        Applies additional transformations to the DataFrame.
        :param df: Source DataFrame.
        :param transformations_needed: List of transformations to apply.
        :return: Transformed DataFrame.
        """
        try:
            if "example_transformation" in transformations_needed:
                pass
            data = [("BAX", 1), ("VAN", 2)]
            df = self.spark.createDataFrame(data, ["Name", "Quantity"])
            return df
        except Exception as e:
            self.logger.error(f"Error applying transformations: {e}")
            raise

    def get_spark_session(self, warehouse_path: str,
                          catalog_name: str = "glue_catalog") -> SparkSession:
        """
        Function to initialize a Spark session with Iceberg configurations.
        :param warehouse_path: S3 path to the Iceberg warehouse.
        :param catalog_name: Name of the Iceberg catalog.
        :return: Configured Spark session.
        """
        try:
            return SparkSession.builder \
                .appName("IcebergApp") \
                .config("spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions"
                        ".IcebergSparkSessionExtensions") \
                .config(f"spark.sql.catalog.{catalog_name}",
                        "org.apache.iceberg.spark.SparkCatalog") \
                .config(f"spark.sql.catalog.{catalog_name}.warehouse",
                        warehouse_path) \
                .config(f"spark.sql.catalog.{catalog_name}.catalog-impl",
                        "org.apache.iceberg.aws.glue.GlueCatalog") \
                .config(f"spark.sql.catalog.{catalog_name}.io-impl",
                        "org.apache.iceberg.aws.s3.S3FileIO") \
                .getOrCreate()
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {e}")
            raise

    def run_transformations(self,
                            df: DataFrame,
                            transformations_needed: List[str]
                            ) -> Tuple[DataFrame, DataFrame]:
        """
        Runs transformations on the DataFrame and returns valid and skipped
        records.
        :param df: Source DataFrame.
        :param transformations_needed: List of transformations to apply.
        :return: Tuple of valid and skipped DataFrames.
        """
        try:
            valid_df = self.apply_transformations(df, transformations_needed)
            skip_df = self.spark.createDataFrame([], df.schema)
            return valid_df, skip_df
        except Exception as e:
            self.logger.error(f"Error running transformations: {e}")
            raise

    def create_table_if_not_exists(self, df: DataFrame,
                                   database_name: str,
                                   table_name: str) -> None:
        """
        Creates an Iceberg table if it does not exist.
        :param df: DataFrame schema to be used for creating the table.
        :param database_name: Name of the database.
        :param table_name: Name of the table.
        """
        catalog_name = "glue_catalog"
        table_name = table_name.lower()
        table = f"{catalog_name}.{database_name}.{table_name}"
        table_path = f"{self.warehouse_path}{table}"
        try:
            self.logger.info(f"Checking if table {table} exists in the "
                             f"database {database_name}...")
            tables = self.spark.catalog.listTables(database_name)
            table_names = [table.name for table in tables]
            self.logger.info(f"Tables in database "
                             f"{database_name}: {table_names}")
            if table_name not in table_names:
                self.logger.info(f"Table {table} does not exist. "
                                 "Proceeding to create...")
                self.logger.debug(f"DataFrame schema to be used: "
                                  f"{df.schema.simpleString()}")
                self.logger.debug(f"Table path: {table_path}")
                df.printSchema()
                df.show(1)
                df.writeTo(table) \
                    .using("iceberg") \
                    .tableProperty("location", table_path) \
                    .tableProperty("write.format.default", "parquet") \
                    .createOrReplace()
                self.logger.info(f"Table {table} created "
                                 f"successfully at {table_path}.")
            else:
                self.logger.info(f"Table {table} already "
                                 "exists. No action required.")
        except Exception as e:
            self.logger.error(f"""An error occurred while trying
                                  to create the table {table}.""")
            self.logger.error(f"Exception details: {str(e)}")
            raise

    def column_exists(self, df: DataFrame, col_name: str) -> bool:
        """
        Checks if a column exists in a DataFrame.
        :param df: DataFrame to check.
        :param col_name: Column name to check for existence.
        :return: True if column exists, False otherwise.
        """
        return col_name in df.columns

    def alter_table_for_missing_columns_api(self, database_name: str,
                                            table_name: str, df: DataFrame):
        """
        Checks the schema of the Iceberg table and adds any missing columns
        from the DataFrame.
        :param database_name: Name of the database.
        :param table_name: Name of the Iceberg table.
        :param df: DataFrame containing the new data with potential new
        columns.
        """
        try:
            # Define the Iceberg catalog and get the table identifier
            catalog = self.spark.catalog("glue_catalog")
            table_identifier = TableIdentifier.of(database_name, table_name)
            iceberg_table: Table = catalog.loadTable(table_identifier)
            existing_columns = set(field.name.lower()
                                   for field in
                                   iceberg_table.schema().columns()
                                   )
            new_columns = [
                (field.name, field.dataType)
                for field in df.schema.fields
                if field.name.lower() not in existing_columns
            ]

            if new_columns:
                update_schema = iceberg_table.updateSchema()
                for column_name, column_type in new_columns:
                    update_schema = update_schema.addColumn(column_name,
                                                            column_type.
                                                            simpleString())
                    self.logger.info(f"""Added column '{column_name}' of type
                                     '{column_type.simpleString()}' to
                                     table '{table_name}'""")
                update_schema.commit()
                self.logger.info(f"""Successfully committed schema
                                     update to Iceberg table '{table_name}'""")
            else:
                self.logger.info(f"""No new columns
                                     to add to Iceberg table '{table_name}'""")

        except Exception as e:
            self.logger.error(f"""Failed to alter Iceberg table 
                                  '{table_name}' for missing columns: {e}""")
            raise

    def add_test_columns(self, df: DataFrame) -> DataFrame:
        """
        Adds new columns with dummy values for testing schema evolution.
        New columns are prefixed with add_<type>, such as add_int,
        add_decimal, add_float, add_string, and add_boolean.

        :param df: Original DataFrame.
        :return: DataFrame with added test columns.
        """
        # Define dummy columns with names and values
        new_columns = [
            ("add_int", IntegerType(), 1),
            ("add_decimal", DecimalType(10, 2), 99.99),
            ("add_float", FloatType(), 1.23),
            ("add_string", StringType(), "test"),
            ("add_boolean", BooleanType(), True)
        ]

        # Add each column if it does not already exist in the DataFrame
        for column_name, data_type, dummy_value in new_columns:
            if column_name not in df.columns:
                df = df.withColumn(column_name, lit(dummy_value).cast(data_type))
                self.logger.info(f"Added test column '{column_name}' of type "
                                 f"'{data_type.simpleString()}' with dummy "
                                 f"value '{dummy_value}'")

        self.logger.info("Test DataFrame with additional columns ready.")
        return df

    def alter_table_for_missing_columns(self, database_name: str,
                                        table_name: str, df: DataFrame,
                                        catalog_name: str = "glue_catalog",
                                        testing_mode: bool = False
                                        ):
        """
        Checks the schema of the Iceberg table and adds any missing
        columns from the DataFrame, using the specified catalog.

        :param database_name: Name of the database.
        :param table_name: Name of the Iceberg table.
        :param df: DataFrame containing the new data with potential
                new columns.
        :param catalog_name: Catalog name (default is 'glue_catalog').
        """
        # Define the full table identifier using the catalog
        full_table_name = f"{catalog_name}.{database_name}.{table_name}"

        try:
            self.logger.info(f"Starting schema evolution check on "
                             f"table: '{full_table_name}'")

            # Fetch existing columns from the Iceberg table
            table_schema = self.spark.table(full_table_name).schema
            existing_columns = {field.name.lower()
                                for field in table_schema.fields}
            self.logger.info(f"Existing columns in '{full_table_name}': "
                             f"{existing_columns}")

            # Identify new columns to add
            new_columns = [(field.name, field.dataType)
                           for field in df.schema.fields
                           if field.name.lower() not in existing_columns]

            # If no new columns and testing_mode is True, add test columns
            if not new_columns and testing_mode:
                self.logger.info("No new columns in DataFrame. Adding test "
                                 "columns for schema evolution testing.")
                df = self.add_test_columns(df)
                # Re-calculate new columns after adding test columns
                new_columns = [(field.name, field.dataType)
                               for field in df.schema.fields
                               if field.name.lower() not in existing_columns]

            # Log identified new columns
            if new_columns:
                new_columns_info = [(col[0], col[1].simpleString())
                                    for col in new_columns]
                self.logger.info(f"New columns to add to '{full_table_name}': "
                                 f"{new_columns_info}")

            # Execute SQL to add missing columns
            for column_name, column_type in new_columns:
                alter_sql = (f"ALTER TABLE {full_table_name} "
                             f"ADD COLUMN {column_name} "
                             f"{column_type.simpleString()}")
                self.logger.info(f"Executing SQL: {alter_sql}")
                try:
                    self.spark.sql(alter_sql)
                    self.logger.info(f"Added column '{column_name}' of type "
                                     f"'{column_type.simpleString()}' to "
                                     f"'{full_table_name}'")
                except Exception as sql_ex:
                    self.logger.error(f"Error executing ALTER TABLE for "
                                      f"column '{column_name}': {sql_ex}")
                    if "HiveException" in str(sql_ex):
                        self.logger.error(f"HiveException may relate to "
                                          f"storage descriptor issues. Check "
                                          f"'{table_name}' in database "
                                          f"'{database_name}' for proper input"
                                          f"format/storage.")
                    raise

            # Log if no new columns are needed
            if not new_columns:
                self.logger.info(f"""No new columns to add
                                     for '{full_table_name}'""")
        except Exception as e:
            self.logger.error(f"""Failed to alter table '
                                  {full_table_name}': {e}""")
            raise
    
    def write(self, df: DataFrame, database_name: str, table_name: str, primary_key: str) -> None:
        """
        Writes a DataFrame to an Iceberg table with upsert capability.

        :param df: DataFrame to be written.
        :param database_name: Name of the database.
        :param table_name: Name of the table.
        :param primary_key: Comma-separated list of primary key columns for the table.
        """
        catalog_name = "glue_catalog"
        table_name = table_name.lower()
        table = f"{catalog_name}.{database_name}.{table_name}"
        
        temp_view_name = f"temp_{table_name}"

        try:
            # Ensure primary keys exist
            primary_keys = [key.strip() for key in primary_key.split(",")]
            if len(primary_keys) > 1:
                unique_key_name = "_".join(primary_keys)
                df = df.withColumn(unique_key_name, concat_ws("_", *[col(key) for key in primary_keys]))
            else:
                unique_key_name = primary_keys[0]
                df = df.withColumn(unique_key_name, col(primary_keys[0]))

            df = df.withColumn("ENV_SRC_CD", lit("GNSTGLBL"))
            df = df.drop("file_timestamp")

            # Add default ETL columns if missing in source DataFrame
            

            # Ensure ETL columns are correctly populated
            df = df.withColumn("ETL_CRT_DTM", current_timestamp())
            df = df.withColumn("ETL_UPDT_DTM", current_timestamp())
            df = df.withColumn("FILE_PATH_NAME", input_file_name())

            df.createOrReplaceTempView(temp_view_name)
            self.logger.info(f"PrintSchema Before merged into Iceberg table: {table}")
            df.printSchema()
            

            # Ensure table exists and add missing columns to target
            self.create_table_if_not_exists(df, database_name, table_name)
            df = self.add_missing_columns_to_source(df, table)
            self.alter_table_for_missing_columns(database_name, table_name, df)
            df.printSchema()
            
            # Merge the data
            columns = ", ".join([f"t.`{c}` = s.`{c}`" for c in df.columns if c not in ["ETL_CRT_DTM", "ETL_UPDT_DTM", unique_key_name]])
            merge_sql = f"""
            MERGE INTO {table} t
            USING {temp_view_name} s
            ON t.`{unique_key_name}` = s.`{unique_key_name}`
            WHEN MATCHED THEN
            UPDATE SET
                t.ETL_UPDT_DTM = current_timestamp(),
                {columns}
            WHEN NOT MATCHED THEN
            INSERT *
            """
            self.spark.sql(merge_sql)
            self.logger.info(f"Data merged into Iceberg table: {table}")

        except Exception as e:
            self.logger.error(f"Error during merge operation: {e}")
            raise

    def execute(self,
                config_path: str) -> None:
        """
        Executes the data transformation process.
        :param config_path: Path to the configuration file.
        """
        self.logger.debug(f"config_path: {config_path}")
        try:
            yaml_config = self.read_config(config_path)
            batch_processor = BatchFileProcessor(
                spark=self.spark,
                yaml_config=yaml_config,
                logger=self.logger,
                config_parser=self.config_parser
            )
            result_df, processed_files = batch_processor.process_files()
            if result_df is None:
                self.logger.info("No files were processed. Exiting.")
                return
            self.logger.info(f"\n{'%' * 100}")
            self.logger.info(f"List of files being processed:{processed_files}")            
            self.logger.info(f"\n{'%' * 100}")
            source_df = result_df
            source_df.printSchema()
            
            self.write(
                df=source_df,
                database_name=yaml_config["target"]["db_name"],
                table_name=yaml_config["target"]["table_name"],
                primary_key=yaml_config["target"]["unique_key"]
            )
            self.logger.info("Write operation complete. Now archiving files.")
            archive_path = yaml_config["archive_path"]
            batch_processor.archive_processed_files(archive_path)
            self.logger.info(f"\n{'*' * 70}")
            self.logger.info("Files archived:")
            for file in processed_files:
                # pylint: disable=W0212
                source_bucket, source_key = batch_processor._parse_s3_path(
                                                                           file
                                                                           )
                target_path = f"{archive_path}/{source_key.split('/')[-1]}"
                self.logger.info(source_bucket)
                self.logger.info(f"""File moved from source:
                                     {file} to target: {target_path}""")
            self.logger.info(f"\n{'*' * 70}")

            self.logger.info("Archiving of processed files is complete.")
        except Exception as e:
            self.logger.error(f"Error executing data transformation: {e}")
            raise