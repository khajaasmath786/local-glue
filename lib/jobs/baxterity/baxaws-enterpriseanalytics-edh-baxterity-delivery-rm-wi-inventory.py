import json
import logging
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, concat_ws
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
from lib.clients.elasticsearch_manager import ElasticSearchManager
from lib.utils.secrets import SecretsManager
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SparkConf
from pyspark.context import SparkContext
from lib.clients.glue import DBConnector
from lib.utils.es_utils import ESUtils

class AthenaQueryExecutor:
    def __init__(self, args, region_name='us-east-2'):
        # Initialize SparkSession
        self.JOB_NAME = args['JOB_NAME']
        self.BP_CD = args['BP_CD']
        self.secret_name = args['secret_name']
        self.es_index_name = args['es_index_name']
        self.id_fields = args['id_fields'].split(',')  # Split the id_fields string into a list
        self.es_utils = ESUtils()
        
        conf = SparkConf()
        conf.set("spark.driver.maxResultSize", "10g")
        self.sc = SparkContext(appName="SnowflakeIntegration", conf=conf)
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.spark.sparkContext.setLogLevel("ERROR")
        self.job = Job(self.glueContext)
        self.job.init(self.JOB_NAME, args)
        self.secret_manager = SecretsManager(args['secret_name'])
        self.sf_options = self.secret_manager.fetch_snowflake_secrets()
        self.glue_client = boto3.client('glue', region_name=region_name)
        self.connector = DBConnector()
        bax_secret_name = "baxaws-enterpriseanalytics-edh-baxterity-opensearch-credentials"
        bax_secrets_manager = SecretsManager(bax_secret_name)
        credentials = bax_secrets_manager.fetch_baxterity_aws_secrets()
        self.es = ElasticSearchManager(self.es_index_name,
                                       credentials.get('es_endpoint'),
                                       credentials.get('es_port'), credentials.get('access_key'),
                                       credentials.get('secret_key'))

    def query_metadata(self):
        """
        Query PostgreSQL and return the DataFrame.
        """
        glue_connection_name = "baxaws-enterpriseanalytics-edh-postgresql"
        query = """
        select max(start_time) from baxmfgdpl.lth_job_audit where job_name = 'baxaws-enterpriseanalytics-edh-baxterity-delivery-rm-wi-inventory' and job_status = 'SUCCESS'
        """

        data = self.connector.query_db(self.spark, self.glue_client, glue_connection_name, query)
        print("here is the date :-", data.show())
        latest_time_stamp = data.collect()[0]['max']
        print("Latest Time Stamp :- ", latest_time_stamp)
        return latest_time_stamp
        
    def get_dataframe(self, jdbc_url, connection_properties, query):
        # Use spark.read to fetch a DataFrame given JDBC details and a SQL query
        return self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("user", connection_properties["user"]) \
            .option("password", connection_properties["password"]) \
            .option("driver", connection_properties["driver"]) \
            .load()
            
    def execute_query(self, jdbc_url, connection_properties, query):
        # Fetch dataframe and show it
        df = self.get_dataframe(jdbc_url, connection_properties, query)
        print("Here is the Dataframe Count ", df.count())
        return df
        
    def execute_query_p_jde_wo_lbr_abspn_rpt(self):
        latest_time_stamp = self.query_metadata()
        connection_properties1 = {
            "user": "GSC_OBS",
            "password": "Eur0v1S1on#2022",
            "driver": "oracle.jdbc.driver.OracleDriver"
        }
        """print("plant_name",self.plant_name)"""
        jdbc_url1 = "jdbc:oracle:thin:@//rgsc1gp.cmqv23cr55f2.us-east-2.rds.amazonaws.com:1521/RGSC1GP"
        q = ('''    SELECT
                    TO_CHAR(FINCL_DT,'YYYY-MM-DD') as FINCL_DT
                    ,ITEM_CD             
                    ,ITEM_DESCR          
                    ,BP_CD               
                    ,BP_DESCR            
                    ,CNTRY_CD            
                    ,CNTRY_DESCR         
                    ,BP_CNTRY_CD         
                    ,BP_CNTRY_DESCR      
                    ,OPRG_GRP2_CD        
                    ,OPRG_GRP2_DESCR    
                    ,XLVL6_CD            
                    ,XLVL6_DESCR         
                    ,XLVL7_CD            
                    ,XLVL7_DESCR         
                    ,MTRL_CLASS_GLBL     
                    ,MTRL_CLASS_DESCR    
                    ,MTRL_CLASS_CTGRY    
                    ,CO_CD               
                    ,CO_GLBL_DESCR       
                    ,MNGR_ENTITY         
                    ,RGN_CD              
                    ,CO_GRP 
                    ,TOT_INV_QTY
                    ,TOT_INV_USD
                    ,OPRG_GRP1_CD
                    ,OPRG_GRP1_DESCR
                    ,PRUOM_CD
                    from GSC_APP_CNTC.GLBL_INV_MYSUIT_ITEM_BP where BP_CD = '%s' and MTRL_CLASS_CTGRY in('RM','WI') 
                    and TO_CHAR(FINCL_DT,'YYYY-MM-DD') >= '%s'
                  ''') % (self.BP_CD,str(latest_time_stamp)) # Ensure query is correctly formatted
					#CAST(DATE_FORMAT('jp_LowEndDateTime', 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)
                    #and FINCL_DT >= TO_DATE('%s', 'YY-MM-DD') -1 
        print(f"Here is the query :- {q}")
        df = self.execute_query(jdbc_url1, connection_properties1, q)
        
        _count = df.count()
        print("Query result count :- ", _count)
        if _count == 0:
            print(" Received 0 count for this load, So skipping job execution")
            return None
            
        print("Conversion starts")
        
        # Create the primary key column by concatenating the ID fields
        df = df.withColumn("primarykey", concat_ws("_", *self.id_fields))

        # Add a row number to each row for batching
        window_spec = Window.orderBy("primarykey")
        df = df.withColumn("row_num", row_number().over(window_spec))

        batch_size = 1000  # Updated batch size to 1000 records
        total_records = df.count()
        num_batches = (total_records // batch_size) + 1
        
        for batch in range(num_batches):
            batch_df = df.filter(
                (col("row_num") > batch * batch_size) & (col("row_num") <= (batch + 1) * batch_size)
            ).drop("row_num")
            
            doc = batch_df.toJSON().map(lambda j: json.loads(j)).collect()
            
            if not doc:
                print(f"Batch {batch + 1} is empty. Skipping.")
                continue

            print(f"Batch {batch + 1} has {len(doc)} records.")

            action = {"update": {"_index": self.es_index_name, "_id": None}}  # Initialize with None ID
            payloads = self.split_payloads_with_id(doc, action)

            for payload in payloads:
                if not payload.strip():
                    print(f"Payload for batch {batch + 1} is empty. Skipping.")
                    continue

                self.es.bulk_insert_with_payload(payload, self.es_index_name)
            print(f"Batch {batch + 1}/{num_batches} processed")

        print(":::: Documents Loaded Successfully ::::: ")

    def split_payloads_with_id(self, docs, action, max_payload_size=8388608):  # 8 MB
        """
        Split payloads into smaller chunks if they exceed the max size.
        Ensure each document has an _id.
        """
        payload_string = ""
        payloads = []
        for doc in docs:
            action["update"]["_id"] = doc.pop("primarykey") # doc["primarykey"] Assign the primary key as the document ID
            payload_string += json.dumps(action) + "\n"
            payload_string += json.dumps({"doc": doc, "doc_as_upsert": True}) + "\n"
            if len(payload_string.encode('utf-8')) > max_payload_size:
                payloads.append(payload_string)
                payload_string = ""
        if payload_string:
            payloads.append(payload_string)
        return payloads

    def execute(self):
        self.execute_query_p_jde_wo_lbr_abspn_rpt()

    def stop(self):
        self.spark.stop()

def main():
    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME', 'secret_name', 'es_index_name', 'BP_CD', 'id_fields'])
    executor = AthenaQueryExecutor(args)
    executor.execute()
    executor.stop()

if __name__ == "__main__":
    main()
