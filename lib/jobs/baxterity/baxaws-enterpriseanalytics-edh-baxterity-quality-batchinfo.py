# Edited by Pooja
#Asmath

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

    def __init__(self, args ,region_name='us-east-2'):
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
        self.SITENAME = args['SITENAME']
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
                                       credentials.get('es_port'), credentials.get('access_key'),
                                       credentials.get('secret_key'))
        

    def execute(self):
        """
        Fetches Snowflake credentials from AWS Secrets Manager.
        Runs the datapipeline
        """
        self.log_parameters()
        # self.list_all_tables()
        # self.setup_snowflake_connection_options()
        # self.insert_dummy_records_to_elasticsearch()
        sf_df = self.fetch_data_from_snowflake()
        self.insert_into_elasticsearch(sf_df)
        #self.read_data_from_view()

  
    
    def insert_dummy_records_to_elasticsearch(self):
        """
        Inserts dummy records into Elasticsearch for demonstration purposes.
        """
        # Define the dummy records as a list of dictionaries
        dummy_records = [
            {"PR_ID": "001", "SEQ_NO": "01", "GRID_NAME": "Dummy Grid 1"},
            {"PR_ID": "002", "SEQ_NO": "02", "GRID_NAME": "Dummy Grid 2"}
        ]

        # Use the ElasticSearchManager to insert these records into the specified index
        if self.es.create_index():
            self.es.insert_document(dummy_records, self.es_index_name)
            logging.info("Dummy data successfully inserted into Elasticsearch")
        else:
            logging.error("Failed to create index and insert dummy data")
            
    def query_metadata(self):
        """
        Query PostgreSQL and return the DataFrame.
        """
        glue_connection_name = (
            "baxaws-enterpriseanalytics-edh-postgresql"
        )
        query = """
        select max(start_time) from baxmfgdpl.lth_job_audit where job_name = 'baxaws-enterpriseanalytics-edh-baxterity-quality-batchinfo' and job_status = 'SUCCESS'
        """
        data = self.connector.query_db(
            self.spark, self.glue_client, glue_connection_name, query)
        print("here is the date :-", data.show())
        return  data
    
    def fetch_data_from_snowflake(self):
        
        #Calling query_metadata function for incremental laod
        df = self.query_metadata()
        latest_time_stamp1 = df.collect()[0]['max']
        latest_time_stamp = latest_time_stamp1.strftime('%Y-%m-%d')
        print("This is the max result from postgres", latest_time_stamp)
        print("SITENAME :-",self.SITENAME)
        """
        Retrieves data from Snowflake using a specific query.
        """
        query = """ 
                    SELECT 
                    STAGE_LOAD_DTM  ,
                    SITENAME           ,
                    BATCHID              ,
                    BATCHNUMBER ,
                    BATCHSTATUS   ,
                    WORKORDERNUMBER  ,
                    WORKORDERSTATUS     ,
                    PRODUCTCODE ,
                    PRODUCTNAME               ,
                    PRODUCTFAMILY             ,
                    SITEGROUPNAME            ,
                    PRODUCTIONDATE        ,
                    PRODUCTIONAREA2       ,
                    QUANTITYORDERED       ,
                    QUANTITYFILLED              ,
                    QUANTITYPACKED          ,
                    QUANTITYSHIPPED         ,
                    QUANTITYRELEASED       ,
                    RELEASEDON     ,
                    RELEASEDBY       ,
                    TARGETCATEGORY           ,
                    to_char(ZERODAY, 'yyyy-MM-dd') ZERODAY             ,
                    RELEASETARGET               ,
                    EXPIRYDATE       ,
                    BATCHACTIVEINDICATORS,
                    SITEID   
                    FROM EA_QSA_DEV.QSA_DATA.VW_GBT_BATCHINFO
                    where SITENAME = '%s' 
                    and ZERODAY is not null 
                    and STAGE_LOAD_DTM >= dateadd('day',-1,'%s'::date)
                """ % (self.SITENAME,str(latest_time_stamp))
        # Set Snowflake connection options using secrets and STAGE_LOAD_DTM >= dateadd('day',-1,'%s'::date) ,str(latest_time_stamp)
        sf_options_specific = self.sf_options.copy()
        sf_options_specific["sfURL"] = "https://baxter.us-east-2.privatelink.snowflakecomputing.com"
        sf_options_specific["sfDatabase"] = "EA_QSA_DEV"
        sf_options_specific["sfSchema"] = "QSA_DATA"
        
        
        dfs = self.spark.read.format("net.snowflake.spark.snowflake") \
            .options(**sf_options_specific) \
            .option("query", query) \
            .load()
        print("here is the count", dfs.count())
        return dfs
        # dfs.show()
        
        # df = dfs.count()
        

    def insert_into_elasticsearch(self, df):
        """
        Inserts data from a DataFrame into Elasticsearch.
        """
        if df.count() > 0:
            documents = df.toJSON().map(lambda j: json.loads(j)).collect()
            #print("Here is the documents output ------ ", documents)
            # index_name = "vw_tw8_pr_grid_dtl_ncr_capa"
            self.es.create_index()
            self.es.insert_documents(documents, self.es_index_name,self.id_field)
            logging.info("Data successfully inserted into Elasticsearch")
        else:
            logging.info("No data to insert into Elasticsearch")

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
        

    def read_data_from_view(self):
        """
        Reads data from a Snowflake view into a DataFrame.
        select * from "EA_QSA_DEV"."QSA_DATA"."VW_MEDDRA_UTIL" limit 10;
        """
        # Ensure the Snowflake options are correctly set
        sf_options_specific = self.sf_options.copy()
        # sf_options_specific["sfURL"] = "https://baxter.us-east-2.privatelink.snowflakecomputing.com"
        sf_options_specific["sfDatabase"] = "EA_QSA_DEV"
        sf_options_specific["sfSchema"] = "QSA_DATA"

        # Define the view name
        # view_name = "VW_GBT_BATCHINFO"
        # Read data from Snowflake using custom SQL query

        query = "select * from VW_GBT_BATCHINFO limit 10"

        self.log_info_msg(f"sf_options_specific ${sf_options_specific}")

        # Load data from the Snowflake view into a DataFrame
        df = self.spark.read \
            .format("net.snowflake.spark.snowflake") \
            .options(**sf_options_specific) \
            .option("query", query) \
            .load()
        df.show()

        # Write data to S3
        output_path = "s3://baxaws-dev-enterpriseanalytics-edh-gabi-translate/Baxterity/"
        df.write.mode("overwrite").json(output_path)
        self.job.commit()

        # Optionally, return the DataFrame for further processing
        return df


def main():
    args = getResolvedOptions(sys.argv, ['environment', 'JOB_NAME', 'secret_name','es_index_name','glue_connection_name','SITENAME','id_field'])
    print(args['environment']) # For example, prints: dev
    connector = GlueJobSnowflakeConnector(args)
    connector.execute()


if __name__ == "__main__":
    main()
