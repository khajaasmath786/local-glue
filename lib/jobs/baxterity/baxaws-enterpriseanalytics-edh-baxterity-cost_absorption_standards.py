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
        self.BU_CD = args['BU_CD']
        self.secret_name = args['secret_name']
        self.es_index_name = args['es_index_name']
        self.drop_field = args['drop_field'].split(',') # Split the drop_field string into a list
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
        select max(start_time) from baxmfgdpl.lth_job_audit where job_name = 'baxaws-enterpriseanalytics-edh-baxterity-cost_absorption_standards' and job_status = 'SUCCESS'
        """

        data = self.connector.query_db(self.spark, self.glue_client, glue_connection_name, query)
        print("here is the date :-", data.show())
        latest_time_stamp = data.collect()[0]['max']
        print("Latest Time Stamp :- ", latest_time_stamp)
        return latest_time_stamp

    def execute_query_p_jde_wo_lbr_abspn_rpt(self):
        latest_time_stamp = self.query_metadata()
        q = (''' select 
                    CST_CMPNT.ITEM_CST_MTHD_CD,
                    CST_CMPNT.CST_CMPNT_CD,
                    CST_CMPNT.BU_CD,
                    CST_CMPNT.SEC_ITEM_NUM,
                    CST_CMPNT.SMLTD_MFG_CST_AMT,
                    CST_CMPNT.SMLTD_CST_ROLLP_AMT,
                    ITEM_BP.GL_CLASS_CD,
                    ITEM_BP.STKG_TYP_CD,
                    ITEM_BP.MSTR_FMLY_PLANG_CD,
                    ITEM_MSTR.PIUOM_CD,
                    ITEM_MSTR.ITEM_01_DESCR,
                    CST_CMPNT.SHRT_ITEM_NUM, --Part of id_fields
                    CST_CMPNT.BP_INV_LCTN_ID, --Part of id_fields
                    CST_CMPNT.LOT_NUM, --Part of id_fields
                    CST_CMPNT.SCD_EXPN_DTM --Part of id_fields
                    From enterpriseanalytics_edh_trnslt_gme.P_JDE_ITEM_CST_CMPNT CST_CMPNT
                    Left Outer Join enterpriseanalytics_edh_trnslt_gme.P_JDE_ITEM_BP ITEM_BP on ITEM_BP.SHRT_ITEM_NUM = CST_CMPNT.SHRT_ITEM_NUM AND CST_CMPNT.BU_CD = ITEM_BP.BU_CD AND ITEM_BP.SCD_CUR_IND = 'Y'
                    Left Outer Join enterpriseanalytics_edh_trnslt_gme.P_JDE_ITEM_MSTR ITEM_MSTR on ITEM_MSTR.SHRT_ITEM_NUM = CST_CMPNT.SHRT_ITEM_NUM  AND ITEM_MSTR.SCD_CUR_IND = 'Y'
                    WHERE CST_CMPNT.ITEM_CST_MTHD_CD = '07' 
                    AND CST_CMPNT.BU_CD = '%s'
                    AND CST_CMPNT.CST_CMPNT_CD in ('C1','C2')
                    AND CST_CMPNT.SCD_CUR_IND = 'Y'
                    AND CST_CMPNT.ETL_UPDT_DTM >= '%s' 
            ''') % (self.BU_CD,str(latest_time_stamp))

        print(f"Here is the query :- {q}")
        df = self.spark.sql(q)
        
        _count = df.count()
        print("Query result count :- ", _count)
        if _count == 0:
            print(" Received 0 count for this load, So skipping job execution")
            return None
            
        print("Conversion starts")

        # Create the primary key column by concatenating the ID fields
        
        df = df.withColumn("primarykey", concat_ws("_", *self.id_fields))
        
        df = df.drop(*self.drop_field) # removed unnecessary columns

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
            print(doc[0])
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
            action["update"]["_id"] = doc.pop("primarykey")#doc["primarykey"] Assign the primary key as the document ID
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
                              ['JOB_NAME', 'secret_name', 'es_index_name', 'BU_CD', 'id_fields','drop_field'])
    executor = AthenaQueryExecutor(args)
    executor.execute()
    executor.stop()

if __name__ == "__main__":
    main()
