#Edited by Pooja

import json
import logging
import sys

import boto3
import requests
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from requests.auth import HTTPBasicAuth

# Asmath

# Setup logging with specific format and info level
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')


class GlueJobSnowflakeConnector:
    """
    A class for connecting AWS Glue jobs to Snowflake using secrets
    stored in AWS Secrets Manager to handle sensitive credentials.
    """

    def __init__(self, args):
        """
        Initializes the GlueJobSnowflakeConnector class with job args.

        Args:
            args: Dictionary of arguments passed to the Glue job.
        """
        self.environment = args['environment']
        self.JOB_NAME = args['JOB_NAME']
        # Assuming JOB_RUN_ID comes from elsewhere as it's not in args
        self.JOB_RUN_ID = ""
        self.secret_name = args['secret_name']
        self.query_tag = 'Glue Job Name: ' + self.JOB_NAME + \
                         ' Job Run Id: ' + self.JOB_RUN_ID
                # Initialize SparkConf
        conf = SparkConf()

        # Conditionally set spark.jars if running locally
        if self.environment == 'local':
            jars_path = "/home/glue_user/workspace/edh_data_reader/jars/"
            spark_jars = "{0}spark-snowflake_2.12-2.10.0-spark_3.1.jar,{0}snowflake-jdbc-3.13.14.jar".format(jars_path)
            conf.set("spark.jars", spark_jars)

        # Initialize SparkContext with the configuration
        self.sc = SparkContext(appName="SnowflakeIntegration", conf=conf)       
        self.glueContext = GlueContext(self.sc)        
        self.spark = self.glueContext.spark_session
        self.spark.sparkContext.setLogLevel("ERROR")
        self.job = Job(self.glueContext)
        self.job.init(self.JOB_NAME, args)
        self.secret_dict = self.fetch_snowflake_secrets()

    def execute(self):
        """
        Fetches Snowflake credentials from AWS Secrets Manager.
        Runs the datapipeline
        """
        self.log_parameters()
        self.setup_snowflake_connection_options()
        self.read_data_from_view()

    def get_connection_details(self, connection_name):
        # Use the Glue API to get connection properties
        glue_client = self.glueContext.get_glue_client()
        connection_info = glue_client.get_connection(Name=connection_name)
        connection_properties = connection_info['Connection']['ConnectionProperties']
        return connection_properties

    # Function to list all tables in the schema
    def list_all_tables(self, connection_name):
        connection_properties = self.get_connection_details(connection_name)
        # Extract JDBC URL, username, and password from connection properties
        jdbc_url = connection_properties['JDBC_CONNECTION_URL']
        # Fetch secret ARN from connection properties to get username and password
        secret_arn = connection_properties['SECRET_ID']
        # Use boto3 client to access Secrets Manager

        session = boto3.session.Session()
        secretsmanager = session.client(service_name='secretsmanager')
        secret_value = secretsmanager.get_secret_value(SecretId=secret_arn)
        credentials = eval(secret_value['SecretString'])  # Assume JSON string
        
        # Create a DataFrame to execute SQL query to list tables
        query = "(SELECT USERNAME AS SCHEMA_NAME FROM ALL_USERS) as schema_list"
        df = self.spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "oracle.jdbc.OracleDriver") \
            .option("dbtable", query) \
            .option("user", credentials['username']) \
            .option("password", credentials['password']) \
            .load()
        
        # Print the results
        df.show()      

    def fetch_snowflake_secrets(self):
        """
        Fetches Snowflake credentials from AWS Secrets Manager.

        Returns:
            A dictionary of the Snowflake credentials.
        """
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager')
        try:
            get_secret_value_response = \
                client.get_secret_value(SecretId=self.secret_name)
            if 'SecretString' in get_secret_value_response:
                secret_dict = json.loads(
                    get_secret_value_response['SecretString'])
            else:
                decoded_binary_secret = base64.b64decode(
                    get_secret_value_response['SecretBinary'])
                secret_dict = json.loads(decoded_binary_secret)
            return secret_dict
        except ClientError as e:
            self.log_error_msg(f"Failed to retrieve secret \
                               {self.secret_name}: {e}")
            sys.exit(1)

    def log_info_msg(self, msg):
        """
        Logs an informational message.

        Args:
            msg: The message to log.
        """
        logging.info(msg)

    def log_error_msg(self, msg):
        """
        Logs an error message.

        Args:
            msg: The error message to log.
        """
        logging.error(msg)

    def log_parameters(self):
        """
        Logs the parameters of the job.
        """
        self.log_info_msg(f"Environment={self.environment}")
        self.log_info_msg(f"Job Name={self.JOB_NAME}")
        self.log_info_msg(f"Job Run ID={self.JOB_RUN_ID}")
        self.log_info_msg(f"Secret Name={self.secret_name}")
        self.log_info_msg(f"Query Tag={self.query_tag}")
        
    def get_mapping(df):
        mapping = []
        for t in df.dtypes:
            val = t + t
            mapping.append(val)

        return mapping


    def read_data_from_view(self):
        """
        Reads data from a Snowflake view into a DataFrame. 
        select * from "EA_QSA_DEV"."QSA_DATA"."VW_MEDDRA_UTIL" limit 10;
        """
        # Ensure the Snowflake options are correctly set
        sf_options_specific = self.sf_options.copy()
        sf_options_specific["sfURL"]="https://baxter.us-east-2.privatelink.snowflakecomputing.com"
        sf_options_specific["sfDatabase"] = "EA_QSA_DEV"
        sf_options_specific["sfSchema"] = "QSA_DATA"
        
        # Define the view name
        #view_name = "VW_GBT_BATCHINFO"
        query = "(select * from VW_GBT_BATCHINFO limit 10)"
        self.log_info_msg(f"sf_options_specific ${sf_options_specific}")
        
        # Load data from the Snowflake view into a DataFrame
        df = self.spark.read \
            .format("net.snowflake.spark.snowflake") \
            .options(**sf_options_specific) \
            .option("query", query) \
            .load()
        #mapping = get_mapping(df)
       
        # Print the DataFrame schema and show the first few rows as a sample
        df.printSchema()
        df.show()
        df = df.toDF()
        output_path = "s3://baxaws-dev-enterpriseanalytics-edh-gabi-translate/Baxterity" 
        df.write.json(output_path, mode = "0verwrite")
        job.commit()

        # Optionally, return the DataFrame for further processing
        return df

    def setup_snowflake_connection_options(self):
        """
        Sets up the connection options for connecting to Snowflake.
        """
        secret_dict = self.secret_dict
        self.sf_options = {
            "sfURL": f"{secret_dict['account']}.snowflakecomputing.com",
            "sfUser": secret_dict['username'],
            "sfPassword": secret_dict['password'],
            # "sfDatabase": secret_dict['database'],
            "sfDatabase": 'SNOWFLAKE_SAMPLE_DATA',
            "sfSchema": 'TPCDS_SF100TCL', # secret_dict['schema'],
            "sfWarehouse": secret_dict['warehouse'],
            "sfRole": secret_dict['role'],
        }
        self.log_info_msg(f"Snow Flake Options: {self.sf_options}")
        return self.sf_options
        
    def list_elasticsearch_indices(self, es_url, es_username, es_password):
        """
        List all indices in an Elasticsearch cluster using the Elasticsearch REST API.
        Args:
        es_url (str): URL to the Elasticsearch cluster API endpoint.
        es_username (str): Username for HTTP Basic Authentication.
        es_password (str): Password for HTTP Basic Authentication.
        Returns:
        str: Returns the response text or an error message.
        """
        full_url = f"{es_url}/_cat/indices?v"
        try:
            response = requests.get(full_url, auth=HTTPBasicAuth(es_username, es_password), verify=True)
            if response.status_code == 200:
                print(f"Passed to retrieve indices. Status code: {response.status_code}, Error: {response.text}")
                print(response.text)
                return response.text
            else:
                print(f"Failed to retrieve indices. Status code: {response.status_code}, Error: {response.text}")
                return f"Failed to retrieve indices. Status code: {response.status_code}, Error: {response.text}"
        except requests.RequestException as e:
            return f"An error occurred: {str(e)}"


def main():
    args = getResolvedOptions(sys.argv, ['environment', 'JOB_NAME','secret_name'])
    print(args['environment'])  # For example, prints: dev
    connector = GlueJobSnowflakeConnector(args)
    end_point="https://vpc-baxaws-ops-dev-bxty-os-btzka3muzmitxhuv6jrprw24za.us-east-2.es.amazonaws.com"
    username="baxadmin"
    password="Baxterity@60042"
    connector.list_elasticsearch_indices(end_point, username, password)
    connector.execute()

if __name__ == "__main__":
    main()
