# Edited By Ankit
''' 
baxaws-enterpriseanalytics-edh-baxterity-cost-productivity-index
After running all the jobs in Postgres need to update the job name as "baxaws-enterpriseanalytics-edh-baxterity-cost_absorption_variance" instead of baxaws-enterpriseanalytics-edh-baxterity-quality-batchinfo in metadata fun 'args ['JOB_NAME', 'secret_name', 'es_index_name','id_field', 'BU_BP_CD'])
'''

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
        self.BU_BP_CD = args['BU_BP_CD']
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
        select max(start_time) from baxmfgdpl.lth_job_audit where job_name = 'baxaws-enterpriseanalytics-edh-baxterity-cost-productivity-index' and job_status = 'SUCCESS'
        """

        data = self.connector.query_db(self.spark, self.glue_client, glue_connection_name, query)
        print("here is the date :-", data.show())
        latest_time_stamp = data.collect()[0]['max']
        print("Latest Time Stamp :- ", latest_time_stamp)
        return latest_time_stamp

    def execute_query_p_jde_wo_lbr_abspn_rpt(self):
        latest_time_stamp = self.query_metadata()
        q = (''' SELECT 
                    trns.WO_DOC_NUM as ORDER_NUMBER,
                    trns.PRNT_SHRT_ITEM_NUM as ITEM,
                    trns.BU_BP_CD as BRANCH,
                    trns.BU_CD as WORK_CENTER,
                    trns.EXPLN_RMRK_TXT as MACHINE,
                    trns.WRK_SHFT_CD as SHIFT,
                    trns.SHIP_QTY/10000 as QUANTITY,
                    trns.OPN_SEQ_NUM,
                    trns.WO_OPN_STS_CD,
                    trns.WO_HR_TYP_CD,
                    trns.WO_TM_TRNS_PRCSD_CD,
                    mstr.ROUTG_TYP_CD AS ROUTIG_TYPE,
                    trns.WO_DOC_TYP_CD,
                    trns.KIT_SEC_ITEM_NUM,
                    trns.CNCL_QTY,
                    trns.TOT_LBR_HR,
                    mstr.WO_STS_CD,
                    mstr.BOM_TYP_CD,
                    0 AS LABOR_HOURS,
                    GL_DT,
                    'STANDARD_LABOR' as HOURS_TYPE, 
                    trns.INTRNL_UNIQ_KEY_ID 
                FROM enterpriseanalytics_edh_trnslt_gme.p_jde_wo_tm_trns trns 
                LEFT JOIN enterpriseanalytics_edh_trnslt_gme.p_jde_wo_mstr mstr ON trns.WO_DOC_NUM = mstr.WO_DOC_NUM           
                WHERE TRIM(trns.BU_BP_CD) = '%s'      
                AND WO_HR_TYP_CD = '4'             
                AND trns.deld_in_src_ind = 'N'
                AND trns.etl_updt_dtm >= ('%s') ''') % (self.BU_BP_CD,str(latest_time_stamp))

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
            action["update"]["_id"] = doc.pop("primarykey") #doc["primarykey"] Assign the primary key as the document ID
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
                              ['JOB_NAME', 'secret_name', 'es_index_name', 'BU_BP_CD', 'id_fields'])
    executor = AthenaQueryExecutor(args)
    executor.execute()
    executor.stop()

if __name__ == "__main__":
    main()
