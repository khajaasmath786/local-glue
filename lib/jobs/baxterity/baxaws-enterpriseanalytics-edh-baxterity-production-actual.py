import json
import logging
import sys
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
        self.JOB_NAME = args['JOB_NAME']
        self.BU_BP_CD = args['BU_BP_CD']
        self.secret_name = args['secret_name']
        self.es_index_name = args['es_index_name']
        self.id_fields = args['id_fields'].split(',')  # Split the id_fields string into a list
        self.es_utils = ESUtils()

        conf = SparkConf()
        conf.set("spark.driver.maxResultSize", "10g")
        conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
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
        select max(start_time) from baxmfgdpl.lth_job_audit where job_name = 'baxaws-enterpriseanalytics-edh-baxterity-production-actual' and job_status = 'SUCCESS'
        """

        data = self.connector.query_db(self.spark, self.glue_client, glue_connection_name, query)
        print("Query result from PostgreSQL:")
        data.show()
        latest_time_stamp = data.collect()[0]['max']
        print("Latest Time Stamp :- ", latest_time_stamp)
        return latest_time_stamp

    def execute_query_p_jde_wo_lbr_abspn_rpt(self):
        latest_time_stamp = self.query_metadata()
        query = f"""
            SELECT
                WO_TM_TRNS_PRCSD_CD,
                WO_DOC_NUM,
                WO_DOC_TYP_CD,
                ADDR_NUM,
                PAYRL_TRNS_NUM,
                PRNT_SHRT_ITEM_NUM,
                --KIT_SEC_ITEM_NUM,
                KIT_THIRD_ITEM_NUM,
                BU_BP_CD,
                BU_CD,
                OPN_SEQ_NUM,
                WO_OPN_STS_CD,
                WO_HR_TYP_CD,
                EXPLN_RMRK_TXT,
                BTCH_NUM,
                DOC_TYP_CD,
                CAST(GL_DT AS DATE) AS GL_DT,
                to_date(OPN_CLK_DT, 'yyyy-MM-dd') AS OPN_CLK_DT,
                OPN_STRT_TM,
                OPN_END_TM,
                WRK_SHFT_CD,
                coalesce(UNIT_NUM,'~') as UNIT_NUM,
                EQUIP_BILLG_RT_AMT,
                EMP_HRLY_RT_AMT,
                TOT_LBR_HR,
                TOT_EQUIP_HR,
                EMP_MISC_PAYMT_AMT,
                SHIP_QTY,
                CNCL_QTY,
                INPT_UOM_CD,
                coalesce(SUMRY_REC_CD,'~') as SUMRY_REC_CD,
                date_format(LAST_UPDTD_DTM, 'yyyy-MM-dd HH:MM:SS') AS LAST_UPDTD_DTM,
                PGM_ID,
                WRK_STN_ID,
                INTRNL_UNIQ_KEY_ID,
                TRNS_RSN_CD,
                TOT_STD_HR,
                coalesce(PRDTN_LN_ID,'~') as PRDTN_LN_ID,
                USER_RSRVD_CD,
                CAST(USER_RSRVD_DT AS DATE) AS USER_RSRVD_DT,
                USER_RSRVD_AMT,
                USER_RSRVD_NUM,
                USER_RSRVD_REF_TXT,
                coalesce(ACTVY_CD,'~') as ACTVY_CD,
                ASSET_ITEM_NUM,
                coalesce(HR_INPT_SRC_CD,'~') as HR_INPT_SRC_CD,
                coalesce(RT_INPT_SRC_CD,'~') as RT_INPT_SRC_CD
            FROM enterpriseanalytics_edh_trnslt_gme.P_JDE_WO_TM_TRNS
            WHERE TRIM(BU_BP_CD) = '{self.BU_BP_CD}' and DELD_IN_SRC_IND = 'N'
            and etl_updt_dtm >= '{latest_time_stamp}'
        """

        try:
            df = self.spark.sql(query)
            print("Query result count :- ", df.count())
            df.show(1)
        except Exception as e:
            print(f"Error executing query: {e}")
            raise e

        print("Conversion starts")

        try:
            # Create the primary key column by concatenating the ID fields
            df = df.withColumn("primarykey", concat_ws("_", *self.id_fields))
            df.show(1)
        except Exception as e:
            print(f"Error creating primary key column: {e}")
            raise e

        try:
            # Add a row number to each row for batching
            window_spec = Window.orderBy("primarykey")
            df = df.withColumn("row_num", row_number().over(window_spec))
            df.show(1)
        except Exception as e:
            print(f"Error adding row number: {e}")
            raise e

        batch_size = 1000  # Updated batch size to 1000 records
        total_records = df.count()
        num_batches = (total_records // batch_size) + 1
        df.show(1)
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
            print("printing payloads")
            payloads = self.split_payloads_with_id(doc, action)
            #print(payloads)

            for payload in payloads:
                #print(f"payload is {payload}")
                if not payload.strip():
                    print(f"Payload for batch {batch + 1} is empty. Skipping.")
                    continue
                # print(f"Inserting payload with bulk insert {payload}")
                response = self.es.bulk_insert_with_payload(payload, self.es_index_name)
                #print(f"Elasticsearch response: {response}")
                if response is not None and response.get("errors"):
                    print(f"Error in Elasticsearch response: {response}")

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
        try:
            self.execute_query_p_jde_wo_lbr_abspn_rpt()
        except Exception as e:
            print(f"Error executing query: {e}")
            raise e

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
