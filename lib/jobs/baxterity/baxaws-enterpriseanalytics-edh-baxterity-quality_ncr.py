import json
import logging
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from lib.clients.glue import DBConnector

from lib.clients.elasticsearch_manager import ElasticSearchManager
from lib.utils.secrets import SecretsManager

# Asmath

# Setup logging with specific format and info level
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')


class GlueJobSnowflakeConnector:
    """
    A class for connecting AWS Glue jobs to Snowflake using secrets
    stored in AWS Secrets Manager to handle sensitive credentials.
    """

    def __init__(self, args, region_name='us-east-2'):
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
        self.glue_client = boto3.client('glue', region_name=region_name)
        self.connector = DBConnector()
        self.es_index_name = args['es_index_name']
        self.id_field = args['id_field']
        self.OWNING_ORGANIZATION = args['OWNING_ORGANIZATION']
        self.glue_connection_name = args['glue_connection_name']
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
        self.secret_manager = SecretsManager(args['secret_name'])
        self.sf_options = self.secret_manager.fetch_snowflake_secrets()
        # self.secret_dict = self.fetch_snowflake_secrets()
        bax_secret_name = "baxaws-enterpriseanalytics-edh-baxterity-opensearch-credentials"
        bax_secrets_manager = SecretsManager(bax_secret_name)
        credentials = bax_secrets_manager.fetch_baxterity_aws_secrets()
        self.es = ElasticSearchManager(self.es_index_name,
                                       credentials.get('es_endpoint'),
                                       credentials.get('es_port'), credentials.get('access_key'), credentials.get('secret_key'))

    def execute(self):
        """
        Fetches Snowflake credentials from AWS Secrets Manager.
        Runs the datapipeline.
        """
        self.log_parameters()
        # self.check_document_count()
        sf_df = self.fetch_data_from_snowflake()
        self.insert_into_elasticsearch(sf_df)

    def query_metadata(self):
        """
        Query PostgreSQL and return the DataFrame.
        """
        glue_connection_name = (
            "baxaws-enterpriseanalytics-edh-postgresql"
        )
        query = """
        select max(start_time) from baxmfgdpl.lth_job_audit where job_name = 'baxaws-enterpriseanalytics-edh-baxterity-quality_ncr' and job_status = 'SUCCESS'
        """
        data = self.connector.query_db(
            self.spark, self.glue_client, glue_connection_name, query)
        print("here is the date :-", data.show())
        return data

    def fetch_data_from_snowflake(self):
        # Calling query_metadata function for incremental load
        df = self.query_metadata()
        latest_time_stamp = df.collect()[0]['max']
        print("This is the max result from postgres", latest_time_stamp)
        """
        Retrieves data from Snowflake using a specific query.
        """
        print("OWNING_ORGANIZATION :- ", self.OWNING_ORGANIZATION)
        query = """ select
                            coalesce(MANUFACTURING_LOCATION_GRID,'~') as MANUFACTURING_LOCATION_GRID,
                            coalesce(LOCAL_CATEGORY_NUM1,'~') as LOCAL_CATEGORY_NUM1,
                            coalesce(LOCAL_CATEGORY_NUM2,'~') as LOCAL_CATEGORY_NUM2,
                            coalesce(DATE_TO_CLOSE,to_date('9999-12-31')) as DATE_TO_CLOSE,
                            cast(DATE_OPENED as date) DATE_OPENED,
                            cast(DATE_EVENT_DISCOVERED as date) DATE_EVENT_DISCOVERED,
                            coalesce(CA_PA_REQUIRED_,'~') as CA_PA_REQUIRED_,
                            coalesce(GRID_NAME,'~') as GRID_NAME,
                            DIVISION,
                            PROJECT_NAME,
                            coalesce(SEQ_NO,0) as SEQ_NO,
                            coalesce(PRODUCT_CODE_PART_NUM_GRID,'~') as PRODUCT_CODE_PART_NUM_GRID,
                            PR_STATUS,
                            coalesce(PRODUCT_INVOLVED_,'~') as PRODUCT_INVOLVED_,
                            coalesce(NONCONFORMANCE_CATEGORY,'~') as NONCONFORMANCE_CATEGORY,
                            coalesce(NONCONFORMANCE_SUB_CATEGORY,'~') as NONCONFORMANCE_SUB_CATEGORY,
                            coalesce(IDENTIFIED_CAS_OR_PAS,'~') as IDENTIFIED_CAS_OR_PAS,
                            coalesce(QUANTITY_UNIT_OF_MEASURE_GRID,'~') as QUANTITY_UNIT_OF_MEASURE_GRID,
                            to_char(DATE_UPDATED, 'yyyy-MM-dd hh:mm:ss') as DATE_UPDATED,
                            coalesce(NONCONFORMANCE_TYPE,'~') as NONCONFORMANCE_TYPE,
                            coalesce(CAST(ORIGINAL_DUE_DATE as DATE),to_date('9999-12-31')) as ORIGINAL_DUE_DATE,
                            PR_ID,
                            coalesce(PRODUCT_LOT_SERIAL_NUM_GRID,'~') as PRODUCT_LOT_SERIAL_NUM_GRID,
                            coalesce(CAST(DATE_CLOSED as DATE),to_date('9999-12-31')) as DATE_CLOSED,
                            IS_CLOSED from EA_QSA_DEV.QSA_DATA.VW_TW8_PR_DTL_NCR_CAPA_BAXTERITY where OWNING_ORGANIZATION = '%s'
                            and cast(date_updated as timestamp) >= to_timestamp('%s')
                """ % (self.OWNING_ORGANIZATION,str(latest_time_stamp))
                
        print("Query for snowflake", query)
        # Set Snowflake connection options using secrets
        # --and cast(date_updated as timestamp) >= to_timestamp('%s') (self.OWNING_ORGANIZATION,str(latest_time_stamp))
        sf_options_specific = self.sf_options.copy()
        sf_options_specific["sfURL"] = "https://baxter.us-east-2.privatelink.snowflakecomputing.com"
        sf_options_specific["sfDatabase"] = "EA_QSA_DEV"
        sf_options_specific["sfSchema"] = "QSA_DATA"

        dfs = self.spark.read.format("net.snowflake.spark.snowflake") \
            .options(**sf_options_specific) \
            .option("query", query) \
            .load()
        print("Here is the count : ", dfs.count())
        return dfs

    def insert_into_elasticsearch(self, df):
        """
        Inserts data from a DataFrame into Elasticsearch.
        """
        print('self.id_field', self.id_field)
        if df.count() > 0:
            documents = df.toJSON().map(lambda j: json.loads(j)).collect()
            # print("Here is the documents output ------ ", documents)
            # index_name = "vw_tw8_pr_grid_dtl_ncr_capa"
            self.es.create_index()
            self.es.insert_documents(documents, self.es_index_name, self.id_field)
            logging.info("Data successfully inserted into Elasticsearch")
        else:
            logging.info("No data to insert into Elasticsearch")

    def check_document_count(self):
        """
        Checks the document count in the Elasticsearch index before and after inserting records.
        """
        count_before = self.es.count_documents()
        self.log_info_msg(f"Document count before insertion: {count_before}")

        sf_df = self.fetch_data_from_snowflake()
        self.print_schema_and_sample(sf_df)
        #self.insert_into_elasticsearch(sf_df)

        count_after = self.es.count_documents()
        self.log_info_msg(f"Document count after insertion: {count_after}")

    def print_schema_and_sample(self, df):
        """
        Prints the schema and sample records of the DataFrame.
        """
        df.printSchema()
        self.log_info_msg("Sample records:")
        sample_records = df.take(5)
        for record in sample_records:
            self.log_info_msg(record)

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


def main():
    args = getResolvedOptions(sys.argv, ['environment', 'JOB_NAME', 'secret_name', 'es_index_name', 'glue_connection_name', 'OWNING_ORGANIZATION', 'id_field'])
    print(args['environment'])  # For example, prints: dev
    connector = GlueJobSnowflakeConnector(args)
    connector.execute()


if __name__ == "__main__":
    main()