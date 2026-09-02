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
        select max(start_time) from baxmfgdpl.lth_job_audit where job_name = 'baxaws-enterpriseanalytics-edh-baxterity-scrap-variance' and job_status = 'SUCCESS'
        """

        data = self.connector.query_db(self.spark, self.glue_client, glue_connection_name, query)
        print("here is the date :-", data.show())
        latest_time_stamp = data.collect()[0]['max']
        print("Latest Time Stamp :- ", latest_time_stamp)
        return latest_time_stamp

    def execute_query_p_jde_wo_lbr_abspn_rpt(self):
        latest_time_stamp = self.query_metadata()
        q = ('''
            select
                SCP.FISCL_YR_NUM FY,
                SCP.GL_PRD_NUM Per_No,
                SCP.BU_BP_CD Branch,
                SCP.WO_DOC_TYP_CD Order_Type,
                SCP.WO_DOC_NUM Order_Number,
                SCP.KIT_SEC_ITEM_NUM Parent_Item_Number,
                SCP.DOC_TYP_CD Doc_Type,
                SCP.SHIP_QTY Quantity_Shipped,
                SCP.RLS_QTY Released_QTY,
                SCP.INPT_UOM_CD Parent_UM,
                SCP.SEC_ITEM_NUM Component_Item_Number,
                SCP.SHFT_CD Shift,
                SCP.EXPLN_RMRK_TXT Machine_ID,
                CAST(SCP.CMPLN_DT as DATE) as Complete_Date,
                SCP.GL_DT GL_Date,
                SCP.REC_CRT_DT Creation_Date,
                SCP.BP_INV_LCTN_ID Location,
                SCP.LOT_NUM Lot_Serial_Number,
                SCP.TRNS_RSN_CD Reason_Code,
                SCP.UNIT_CST_AMT Unit_Cost,
                SCP.STD_REQD_QTY Usage_Qty,
                SCP.UOM_CD Component_UM,
                SCP.WO_01_QTY Standard_Scrap_Qty,
                SCP.WO_04_QTY BOM_Material_Chg_Qty,
                SCP.WO_05_QTY Reported_Scrap_Qty,
                SCP.GL_AMT Usage_Amt,
                SCP.TRNS_CRNCY_CD Cur_Cod,
                SCP.NET_POSTG_01_AMT Standard_Scrap_Amt,
                SCP.NET_POSTG_04_AMT BOM_Material_Chg_Amt,
                SCP.NET_POSTG_05_AMT Reported_Scrap_Amt,
                SCP.TRNS_ORIGR_ID Transaction_Originator,
                SCP.USER_RSRVD_CD WO_Status,
                BP.ITEM_POOL_CD Item_Pool,
                BP.GL_CLASS_CD GL_Cat,
                BP.STKG_TYP_CD Stocking_Type,
                WO_05_QTY as Total_Actual_Scrap_Qty,
                NET_POSTG_05_AMT as Total_Actual_Scrap_Amt, 
                (WO_01_QTY-WO_05_QTY) as Variance_Qty,
                (NET_POSTG_01_AMT -NET_POSTG_05_AMT) as Variance_Amt,
                WO_01_QTY/(case when STD_REQD_QTY=0 then 1 else STD_REQD_QTY end) as Standard,
                WO_05_QTY/(case when STD_REQD_QTY=0 then 1 else STD_REQD_QTY end) AS Actual,
                KIT_SEC_ITEM_NUM AS Parent_Item_Number_Description,
                SCP.SEC_ITEM_NUM as Component_Item_Number_Description,
                TRNS_RSN_CD as Reason_Code2,
                SCP.SHRT_ITEM_NUM,
                SCP.CENTRY_NUM,
                SCP.OPN_SEQ_NUM,
                SCP.DATA_SRC_CLASS_CD,
                SCP.FILLG_LN_CD,
                SCP.INTRNL_UNIQ_KEY_ID,
                SCP.REC_CRT_TM
            from
                enterpriseanalytics_edh_trnslt_gme.P_JDE_WO_CMPNT_SCRAP SCP
                inner join enterpriseanalytics_edh_trnslt_gme.P_JDE_ITEM_BP BP on SCP.PRNT_SHRT_ITEM_NUM = BP.SHRT_ITEM_NUM
                and SCP.BU_BP_CD = BP.BU_CD
            where
                SCP.BU_BP_CD = '%s' and SCP.DELD_IN_SRC_IND = 'N' 
                and SCP.ETL_UPDT_DTM >= '%s'
            ''') % (self.BU_BP_CD,str(latest_time_stamp))

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
                              ['JOB_NAME', 'secret_name', 'es_index_name', 'BU_BP_CD', 'id_fields'])
    executor = AthenaQueryExecutor(args)
    executor.execute()
    executor.stop()

if __name__ == "__main__":
    main()
