import boto3
import yaml
import logging
import re
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_date, to_timestamp, date_format
import pyspark.sql.functions as f
from pyspark.sql.functions import lit, to_date, to_timestamp
from typing import Tuple

class ConfigParser:
    """
    Loading, validating, and parsing the partner-specific config.

    Methods:
    read_yaml_config()
    validate_yaml_config()
    get_source_schema()
    transformations_to_run()
    get_source_file_metadata()
    map_spark()
    convert_source_schema_col()
    """

    def __init__(self, s3_bucket, s3_key):
        """
        Initializes the Config class.

        Args:
            s3_bucket: The name of the S3 bucket.
            s3_key: The key of the config file in the S3 bucket.
        """
        # Ensure bucket name does not have the 's3://' prefix
        if s3_bucket.startswith('s3://'):
            s3_bucket = s3_bucket[5:]
        
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_client = boto3.client('s3', 'us-east-2')
    
    def get_serializable_config(self) -> dict:
        """
        Returns a serializable version of the ConfigParser object.
        Excludes non-serializable attributes like the S3 client.

        Returns:
            dict: A dictionary containing the serializable attributes.
        """
        return {
            "schema_mapping": getattr(self, "schema_mapping", {}),
            "logger": None,  # Exclude logger or replace with its string representation if needed
            "other_required_keys": {},  # Add any other keys that you need for the worker
            # Include any additional attributes that are serializable and necessary
        }


    def read_yaml_config(self):
        print(self.s3_bucket)
        print(self.s3_key)
        
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
            config_data = response['Body'].read().decode('utf-8')                        
            yaml_doc = yaml.safe_load(config_data)
            print(yaml_doc)
            return yaml_doc
        except self.s3_client.exceptions.NoSuchKey:
            print(f"The object {self.s3_key} does not exist in bucket {self.s3_bucket}.")
            return None
        except Exception as e:
            print(f"Error fetching the object :: {self.s3_key} from bucket {self.s3_bucket}: {e}")
            return None

    @staticmethod
    def validate_yaml_config(file) -> bool:
        """
        Validates YAML config.

        Returns:
        bool: True if the validation is successful, otherwise raises an error
        """
        expected_keys = ["columns", "file", "transformation_order"]
        if sorted(file.keys()) != expected_keys:
            error = f"""The yaml file had these keys {file.keys()} but expects
                        these {expected_keys} ."""
            logging.error(error)
            raise Exception(error)
        else:
            return True

    def get_source_schema(self, file, source_df=None) -> Tuple[dict, StructType]:
        """
        Using the yaml_file, it builds an expected column/schema.

        Parameters:
        file (dictionary): file contents
        source_df (DataFrame): DataFrame to infer schema if columns are missing

        Returns:
        dictionary: a dict of the schema mapping needed
        StructType: if no header is present this spark structtype will be used
        """
        schema_mapping = {}
        columns = []
        for source_name, standard_names in file["columns"].items():
            schema_mapping[source_name] = standard_names
            if "default" not in standard_names:
                column = {
                    "column_name": source_name,
                    "column_type": standard_names["datatype"],
                }
                columns.append(column)
        
        # If columns are missing from YAML config, infer from source_df
        if source_df:
            for field in source_df.schema.fields:
                if field.name not in schema_mapping:
                    schema_mapping[field.name] = {"bax_standard": field.name, "datatype": field.dataType.simpleString()}
                    columns.append({"column_name": field.name, "column_type": field.dataType.simpleString()})
        
        schema = StructType(
            [
                StructField(c["column_name"], self.map_spark(c["column_type"]), True)
                for c in columns
            ]
        )
        return schema_mapping, schema

    @staticmethod
    def transformations_to_run(file) -> list:
        """
        Using the yaml_file, creates a transformations order to run.

        Parameters:
        file (dictionary): file contents

        Returns:
        list: a list of the transformations needed and ordered correctly
        """
        transformations_needed = []
        for value in file["transformation_order"]:
            transformations_needed.append(value.lower())
        return transformations_needed

    @staticmethod
    def get_source_file_metadata(file) -> dict:
        file_info = {}
        for type, info in file["file"].items():
            file_info[type] = info
        return file_info

    def map_spark(self, column_type) -> dict:
        """
        Maps datatypes correctly for a spark schema.
        """
        number_regex = re.compile("[0-9]+")
        if "decimal" in column_type:
            precision, scale = [
                int(number) for number in number_regex.findall(column_type)
            ]
            column_type = "decimal"
        else:
            precision, scale = None, None
        mapping = {
            "long": LongType(),
            "boolean": BooleanType(),
            "string": StringType(),
            "date": DateType(),
            "double": DoubleType(),
            "binary": BinaryType(),
            "decimal": DecimalType(precision, scale),
            "float": FloatType(),
            "timestamp": TimestampType(),
        }
        return mapping[column_type]

    @staticmethod
    def convert_source_schema_col(df: DataFrame, schema_mapping: dict) -> DataFrame:
        """
        Converts source dataframe to add defaults.

        Parameters:
        column_order (string): correct order of columns
        schema_mapping (dict) : dict with correct column names and default values to map

        Returns:
        dataframe: returns converted dataframe with correct edo column names and added default values
        """
        column_order = df.schema.names
        if schema_mapping:
            for source_name, standard_names in schema_mapping.items():
                if source_name in column_order:
                    df = df.withColumnRenamed(
                        source_name, standard_names["bax_standard"]
                    )
                else:
                    default_value = standard_names.get("default", "")
                    
                    # Check the data type for the target column
                    if standard_names.get("datatype") == "date":
                        # Convert the default value to a proper date format
                        df = df.withColumn(
                            standard_names["bax_standard"],
                            to_date(lit(default_value), "MMM-dd-yyyy")
                        )
                    elif standard_names.get("datatype") == "timestamp":
                        # Convert the default value to a proper timestamp format
                        df = df.withColumn(
                            standard_names["bax_standard"],
                            to_timestamp(lit(default_value), "MMM-dd-yyyy")
                        )
                    else:
                        df = df.withColumn(
                            standard_names["bax_standard"],
                            lit(default_value),
                        )
        else:
            error = "The schema mapping is empty"
            logging.error(error)
            raise Exception(error)
        return df

    """
    Loading, validating, and parsing the partner-specific config.

    Methods:
    read_yaml_config()
    validate_yaml_config()
    get_source_schema()
    transformations_to_run()
    get_source_file_metadata()
    map_spark()
    convert_source_schema_col()
    """

    def __init__(self, s3_bucket, s3_key):
        """
        Initializes the Config class.

        Args:
            s3_bucket: The name of the S3 bucket.
            s3_key: The key of the config file in the S3 bucket.
        """
        if s3_bucket.startswith('s3://'):
            s3_bucket = s3_bucket[5:]
        
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_client = boto3.client('s3', 'us-east-2')
        self.logger = logging.getLogger(__name__)

    def read_yaml_config(self):
        """
        Reads the YAML configuration from S3.

        Returns:
        dict: YAML file contents
        """
        self.logger.info(f"Reading config from {self.s3_bucket}/{self.s3_key}")
        
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
            config_data = response['Body'].read().decode('utf-8')                        
            yaml_doc = yaml.safe_load(config_data)
            self.logger.info(f"YAML config loaded: {yaml_doc}")
            return yaml_doc
        except self.s3_client.exceptions.NoSuchKey:
            self.logger.error(f"The object {self.s3_key} does not exist in bucket {self.s3_bucket}.")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching the object {self.s3_key} from bucket {self.s3_bucket}: {e}")
            return None

    @staticmethod
    def validate_yaml_config(file) -> bool:
        """
        Validates YAML config.

        Returns:
        bool: True if the validation is successful, otherwise raises an error
        """
        expected_keys = ["columns", "file", "transformations_needed", "source_path", "target_path", "skip_target_path"]
        if sorted(file.keys()) != expected_keys:
            error = f"The yaml file had these keys {file.keys()} but expects these {expected_keys}."
            logging.error(error)
            raise Exception(error)
        else:
            return True

    def get_source_schema(self, file, source_df: DataFrame) -> Tuple[dict, list]:
        """
        Using the yaml_file, it builds an expected column/schema.

        Parameters:
        file (dictionary): file contents
        source_df (DataFrame): The source DataFrame to infer the schema if not provided

        Returns:
        dictionary: a dict of the schema mapping needed
        list: a list of dictionaries for the schema if no header is present this spark structtype will be used
        """
        schema_mapping = {}
        schema = []
        
        for source_name, standard_names in file["columns"].items():
            schema_mapping[source_name] = standard_names
            schema.append({
                "name": standard_names.get("bax_standard", source_name),
                "type": standard_names.get("datatype", "string")
            })

        for source_field in source_df.schema.fields:
            if source_field.name not in schema_mapping:
                schema.append({
                    "name": source_field.name,
                    "type": source_field.dataType.simpleString()
                })

        return schema_mapping, schema

    @staticmethod
    def transformations_to_run(file) -> list:
        """
        Using the yaml_file, creates a transformations order to run.

        Parameters:
        file (dictionary): file contents

        Returns:
        list: a list of the transformations needed and ordered correctly
        """
        transformations_needed = []
        if file["transformations_needed"]:
            transformations_needed = ["cardholder_status_conversion", "clean_zip_code"]
        return transformations_needed

    @staticmethod
    def get_source_file_metadata(file) -> dict:
        """
        Extracts file metadata from the YAML configuration.

        Parameters:
        file (dictionary): file contents

        Returns:
        dict: A dictionary of file metadata
        """
        file_info = {}
        for type, info in file["file"].items():
            file_info[type] = info
        return file_info

    def map_spark(self, column_type) -> DataType:
        """
        Maps datatypes correctly for a spark schema.
        """
        number_regex = re.compile("[0-9]+")
        if "decimal" in column_type:
            precision, scale = [
                int(number) for number in number_regex.findall(column_type)
            ]
            column_type = "decimal"
        else:
            precision, scale = None, None
        mapping = {
            "long": LongType(),
            "boolean": BooleanType(),
            "string": StringType(),
            "date": DateType(),
            "double": DoubleType(),
            "binary": BinaryType(),
            "decimal": DecimalType(precision, scale),
            "float": FloatType(),
            "timestamp": TimestampType(),
        }
        return mapping.get(column_type, StringType())

    @staticmethod
    def convert_source_schema_col(df: DataFrame, schema_mapping: dict) -> DataFrame:
        """
        Converts source dataframe to add defaults.

        Parameters:
        df (DataFrame): Source dataframe
        schema_mapping (dict) : Dict with correct column names and default values to map

        Returns:
        DataFrame: Returns converted dataframe with correct column names and added default values
        """
        column_order = df.schema.names
        if schema_mapping:
            for source_name, standard_names in schema_mapping.items():
                if source_name in column_order:
                    df = df.withColumnRenamed(
                        source_name, standard_names["bax_standard"]
                    )
                else:
                    df = df.withColumn(
                        standard_names["bax_standard"],
                        f.lit(standard_names.get("default", "")),
                    )
        else:
            error = "The schema mapping is empty"
            logging.error(error)
            raise Exception(error)
        return df