"""
Processes files in batches, applies schema transformation,
ranks them based on primary key and timestamp, and filters
rows to retain only those with rank 1.
"""

import boto3
import sys
from pyspark.sql.functions import input_file_name, lit
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number, desc, lit, to_date, to_timestamp, coalesce, regexp_replace, date_format
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, TimestampType
from typing import List, Tuple, Optional
from functools import reduce
import sys


class BatchFileProcessor:
    """
    Processes files in batches, applies schema transformation,
    ranks them based on primary key and timestamp, and filters
    rows to retain only those with rank 1.
    """

    def __init__(self, spark: SparkSession, yaml_config: dict, logger, config_parser):
        """
        Initializes BatchFileProcessor.
        :param spark: Spark session.
        :param yaml_config: Config dictionary from YAML.
        :param logger: Logger instance.
        :param config_parser: Config parser for schema mappings.
        """
        self.spark = spark
        self.yaml_config = yaml_config
        self.logger = logger
        self.config_parser = config_parser
        self.s3_client = boto3.client('s3', 'us-east-2')
        self.batch_size = yaml_config.get("batch_size", 100)
        self.processed_files = []

    def archive_processed_files(self, archive_path: str):
        """
        Archives the processed files to the specified archive S3 path.
        """
        for file_path in self.processed_files:
            try:
                bucket_name, file_key = self._parse_s3_path(file_path)
                file_name = file_key.split("/")[-1]
                archive_key = f"{archive_path}/{file_name}"
                archive_bucket, archive_file_key = self._parse_s3_path(f"s3://{archive_key}")
                self.logger.info(f"Archiving file: s3://{bucket_name}/{file_key} to s3://{archive_bucket}/{archive_file_key}")
                self.transfer_files(bucket_name, file_key, archive_bucket, archive_file_key)
            except Exception as e:
                self.logger.error(f"Failed to archive {file_path} due to S3 error: {e}")

    def transfer_files(self, source_bucket, source_key, target_bucket, target_key):
        """
        Transfers files from one S3 location to another.
        """
        try:
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.s3_client.copy_object(CopySource=copy_source, Bucket=target_bucket, Key=target_key)
            self.s3_client.delete_object(Bucket=source_bucket, Key=source_key)
            self.logger.info(f"File transferred from {source_bucket}/{source_key} to {target_bucket}/{target_key}")
        except Exception as e:
            self.logger.error(f"Failed to transfer file: {e}")
            raise

    def _parse_s3_path(self, s3_path: str) -> Tuple[str, str]:
        """
        Parses an S3 path into its bucket and key components.
        """
        path_parts = s3_path.replace("s3://", "").split("/", 1)
        return path_parts[0], path_parts[1]

    def process_files(self) -> Tuple[Optional[DataFrame], List[str]]:
        """
        Processes files, applies schema transformation, ranks rows, and returns a DataFrame.
        """
        source_path = self.yaml_config["source_path"]
        sorted_files = self.get_files_sorted_by_timestamp(source_path)
        if not sorted_files:
            self.logger.info("No files to process.")
            return None, []
        file_paths = [file[0] for file in sorted_files]
        file_timestamps = {file[0]: file[1] for file in sorted_files}  # Map path -> timestamp
        self.logger.info(f"Files to process: {file_paths}")
        self.logger.info(f"Processing {len(file_paths)} files in parallel.")
        processed_df = self.spark.read.option("header", self.yaml_config["file"]["header"]) \
            .option("sep", self.yaml_config["file"]["delimiter"]).option("quote", "\"").option("multiLine", "true").option("escape", "\"") \
            .csv(file_paths)
        processed_df = processed_df.withColumn("SRC_FILE_PATH", input_file_name())
        # processed_df.printSchema()
        self.logger.info(f"Count after reading files: {processed_df.count()}")
        primary_keys = self._get_primary_keys()
        timestamp_df = self.spark.createDataFrame(
            [(path, timestamp) for path, timestamp in file_timestamps.items()],
            ["SRC_FILE_PATH", "FILE_TIMESTAMP"]
        )
        self.logger.info("Timestamp DataFrame created:")
        timestamp_df.show(truncate=False)
        processed_df = processed_df.join(timestamp_df, on="SRC_FILE_PATH", how="left")
        processed_df = processed_df.selectExpr(*[f"`{col}` AS `{col.upper()}`" for col in processed_df.columns])
        # processed_df.printSchema()
        self.logger.info(f"Count after join: {processed_df.count()}")
        columns_to_log = primary_keys + ["FILE_TIMESTAMP", "SRC_FILE_PATH"]
        self.logger.info("Data after join (Primary keys, FILE_TIMESTAMP, SRC_FILE_PATH):")
        

        # Apply transformations and deduplication
        schema_mapping, schema = self.config_parser.get_source_schema(self.yaml_config, processed_df)
        transformed_df = self.source_transformation(processed_df, schema_mapping, schema)
        
        self.logger.info("Data after transformation (Primary keys, FILE_TIMESTAMP, SRC_FILE_PATH):")
        transformed_df.select(*columns_to_log).show(truncate=False)
        transformed_df.printSchema()
        self.logger.info(f"Count after transformation: {transformed_df.count()}")
        
        deduplicated_df = self.log_and_deduplicate(transformed_df, primary_keys)
        self.logger.info(f"Count after deduplication: {deduplicated_df.count()}")
        self.logger.info("Data after deduplication (Primary keys, file_timestamp, src_file_path):")
        
        self.processed_files = file_paths
        deduplicated_df.printSchema()
        deduplicated_df.select(*columns_to_log).show(truncate=False)        
        return deduplicated_df, self.processed_files
    
    def process_files_xx(self) -> Tuple[Optional[DataFrame], List[str]]:
        """
        Processes files, applies schema transformation, ranks rows, and returns a DataFrame.
        """
        source_path = self.yaml_config["source_path"]
        sorted_files = self.get_files_sorted_by_timestamp(source_path)
        if not sorted_files:
            self.logger.info("No files to process.")
            return None, []

        processed_dfs = []
        for i in range(0, len(sorted_files), self.batch_size):
            batch_files = sorted_files[i:i + self.batch_size]
            batch_df = self._read_transform_rank_files(batch_files)
            if batch_df:
                processed_dfs.append(batch_df)
                self.processed_files.extend([file[0] for file in batch_files])

        if processed_dfs:
            result_df = processed_dfs[0]
            for df in processed_dfs[1:]:
                result_df = result_df.union(df)
            return result_df, self.processed_files
        return None, []

    def log_and_deduplicate(self, df: DataFrame, primary_keys: List[str]) -> DataFrame:
        """
        Identifies and logs duplicate rows based on primary keys, then removes duplicates.
        """
        window_spec = Window.partitionBy(*primary_keys).orderBy(desc("file_timestamp"))
        ranked_df = df.withColumn("row_num", row_number().over(window_spec))
        duplicates_df = ranked_df.filter(col("row_num") > 1)
        duplicate_count = duplicates_df.count()
        if duplicate_count > 0:
            self.logger.info(f"Found {duplicate_count} duplicate rows based on primary keys.")
            # Select relevant columns to log
            log_columns = primary_keys + ["row_num", "src_file_path", "file_timestamp"]            
            # Check which columns exist in the DataFrame
            log_columns = [col for col in log_columns if col in duplicates_df.columns]            
            # Show duplicates with the relevant columns
            duplicates_df.select(*log_columns).show(truncate=False)            
            ranked_df = ranked_df.filter(col("row_num") == 1)
        
        return ranked_df

    def archive_processed(self, archive_path: str):
        """
        Archives processed files to an S3 path, preserving directory structure.
        """
        for file_path in self.processed_files:
            try:
                bucket_name, file_key = self._parse_s3_path(file_path)
                sub_path = "/".join(file_key.split("/")[1:])
                archive_key = f"{archive_path}/{sub_path}"
                self.s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': file_key}, Key=archive_key)
                self.s3_client.delete_object(Bucket=bucket_name, Key=file_key)
                self.logger.info(f"Archived file {file_path} to {archive_key}")
            except Exception as e:
                self.logger.error(f"Failed to archive {file_path}: {e}")

    def get_files_sorted_by_timestamp(self, s3_path: str) -> List[Tuple[str, str]]:
        """
        Retrieves and sorts files from S3 based on timestamp.
        """
        bucket_name, prefix = self._parse_s3_path(s3_path)
        response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            return []
        files = response['Contents']
        files_sorted = sorted(files, key=lambda x: x['LastModified'])
        return [(f"s3://{bucket_name}/{file['Key']}", file['LastModified']) for file in files_sorted]

    def _read_transform_rank_files(self, files_with_timestamps: List[Tuple[str, str]]) -> DataFrame:
        """
        Reads files, transforms schema, ranks rows, and filters to retain only rank 1 rows.
        """
        all_columns = set()
        dfs = []
        for file_path, file_timestamp in files_with_timestamps:
            df = self.read_file(file_path, self.yaml_config["file"]["header"], self.yaml_config["file"]["delimiter"])
            df = df.withColumn("file_timestamp", lit(file_timestamp)).withColumn("src_file_path", lit(file_path))
            schema_mapping, schema = self.config_parser.get_source_schema(self.yaml_config, df)
            df = self.source_transformation(df, schema_mapping, schema)
            all_columns.update(df.columns)
            primary_keys = self._get_primary_keys()
            df = self.log_and_deduplicate(df, primary_keys)
            dfs.append(df)

        master_schema_columns = sorted(all_columns)
        aligned_dfs = [self.align_to_schema(df, master_schema_columns) for df in dfs]
        combined_df = reduce(DataFrame.union, aligned_dfs)
        return self.log_and_deduplicate(combined_df, self._get_primary_keys())

    def read_file(self, file_path: str, header: str, delimiter: str) -> DataFrame:
        """
        Reads a CSV file into a DataFrame.
        """
        try:
            # df = self.spark.read.option("header", header).option("sep", delimiter) \
            df = self.spark.read.option("header", header) \
                .option("sep", delimiter) \
                .option("quote", "\"").option("multiLine", "true") \
                .option("escape", "\"").csv(source_path)
            return df
        except Exception as e:
            self.logger.error(f"Failed to read file {source_path}: {e}")
            raise

    def _get_primary_keys(self) -> List[str]:
        """
        Retrieves primary key columns from the config.
        """
        return [key.strip() for key in self.yaml_config["target"]["unique_key"].split(",")]

    def rename_columns(self, df: DataFrame, schema_mapping: dict) -> DataFrame:
        """
        Renames columns in a DataFrame based on schema mapping.
        """
        for source_col, target_col in schema_mapping.items():
            if source_col in df.columns:
                df = df.withColumnRenamed(source_col, target_col["bax_standard"])
        return df

    def confirm_column_count(self, renamed_df: DataFrame, typecasted_df: DataFrame) -> None:
        """
        Confirms the total columns in the renamed and typecasted DataFrame.
        """
        if len(renamed_df.columns) != len(typecasted_df.columns):
            raise ValueError("Column count mismatch between renamed and typecasted DataFrames.")

    def confirm_column_names(self, renamed_df: DataFrame, typecasted_df: DataFrame) -> None:
        """
        Confirms if column names are the same between the renamed and typecasted DataFrames.
        """
        if set(renamed_df.columns) != set(typecasted_df.columns):
            raise ValueError("Column names mismatch between renamed and typecasted DataFrames.")

    def change_column_types(self, df: DataFrame, schema: List[dict]) -> DataFrame:
        """
        Changes column types in a DataFrame based on schema.
        """
        for col_info in schema:
            col_name, col_type = col_info["name"], col_info["type"]
            if col_name in df.columns:
                if col_type == "date":
                    df = df.withColumn(col_name, to_date(col(col_name)))
                elif col_type == "timestamp":
                    df = df.withColumn(col_name, to_timestamp(col(col_name)))
                else:
                    df = df.withColumn(col_name, col(col_name).cast(col_type))
            else:
                df = df.withColumn(col_name, lit(None).cast(col_type))
        return df

    def source_transformation(self, df: DataFrame, schema_mapping: dict, schema: List[dict]) -> DataFrame:
        """
        Transforms a DataFrame based on schema and mapping.
        """
        renamed_df = self.rename_columns(df, schema_mapping)
        typecasted_df = self.change_column_types(renamed_df, schema)
        return typecasted_df

    def align_to_schema(self, df: DataFrame, master_schema_columns: List[str]) -> DataFrame:
        """
        Aligns a DataFrame to a master schema.
        """
        for col in master_schema_columns:
            if col not in df.columns:
                df = df.withColumn(col, lit(None))
        return df.select(master_schema_columns)