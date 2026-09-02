# Edited By Asmath
'''After running all the jobs in Postgres need to update the job name as "baxaws-enterpriseanalytics-edh-baxterity-scrap-variance" instead of baxaws-enterpriseanalytics-edh-baxterity-quality-batchinfo in metadata fun '''

import json
import logging
import sys
from pyspark.sql import SparkSession
import sys
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from lib.clients.elasticsearch_manager import ElasticSearchManager
from lib.utils.secrets import SecretsManager
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.exceptions import ClientError
from pyspark import SparkConf
from pyspark.context import SparkContext
from lib.clients.glue import DBConnector
import pandas as pd
from pyspark.sql.functions import date_format


class AthenaQueryExecutor:
    def __init__(self, args, region_name='us-east-2'):
        # Initialize SparkSession
        self.JOB_NAME = args['JOB_NAME']
        self.secret_name = args['secret_name']
        self.es_index_name = args['es_index_name']
        self.BU_CTGRY_CD= args['BU_CTGRY_CD']
        self.FISCL_YR_NUM= args['FISCL_YR_NUM']
        self.id_field = args['id_field']
        conf = SparkConf()
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
        glue_connection_name = (
            "baxaws-enterpriseanalytics-edh-postgresql"
        )
        query = """
        select max(start_time) from baxmfgdpl.lth_job_audit where job_name = 'baxaws-enterpriseanalytics-edh-baxterity-cost-overhead-variance' and job_status = 'SUCCESS'
        """

        data = self.connector.query_db(
            self.spark, self.glue_client, glue_connection_name, query)
        print("here is the date :-", data.show())
        latest_time_stamp = data.collect()[0]['max']
        print("Latest Time Stamp :- ", latest_time_stamp)

        return data

    def execute_query_p_jde_wo_lbr_abspn_rpt(self):
        latest_time_stamp = self.query_metadata()
        print("Here latest_time_stamp in function :", latest_time_stamp)
        latest_time_stamp.createOrReplaceTempView('max_table')
        sq = '''select * from max_table'''
        result = self.spark.sql(sq)
        doc = result.toJSON().map(lambda j: json.loads(j)).collect()
        print("Json file result of temporary view :- ", doc)
        q = ('''
                select ACCT_MSTR.ACCT_ID,date_format(ACCT_MSTR.SCD_EXPN_DTM, 'yyyy-MM-dd HH:MM:SS') as SCD_EXPN_DTM,ACCT_BAL.CENTRY_NUM,ACCT_BAL.FISCL_YR_NUM,ACCT_BAL.FISCL_QTR_CD,ACCT_BAL.SL_CD,ACCT_BAL.LDGR_TYP_CD,ACCT_BAL.SL_TYP_CD,ACCT_BAL.TRNS_CRNCY_CD,
                        BU.BU_01_DESCR 
                        ,BU.BU_CD 
                        ,ACCT_MSTR.OBJ_ACCT_CD
                        ,NET_POSTG_01_AMT AS  JAN
                        ,NET_POSTG_02_AMT AS  FEB 
                        ,NET_POSTG_03_AMT AS  MAR 
                        ,NET_POSTG_04_AMT AS  APR 
                        ,NET_POSTG_05_AMT AS  MAY 
                        ,NET_POSTG_06_AMT AS  JUNE
                        ,NET_POSTG_07_AMT AS  JUL 
                        ,NET_POSTG_08_AMT AS  AUG 
                        ,NET_POSTG_09_AMT AS  SEP 
                        ,NET_POSTG_10_AMT AS  OCT 
                        ,NET_POSTG_11_AMT AS  NOV 
                        ,NET_POSTG_12_AMT + NET_POSTG_13_AMT + NET_POSTG_14_AMT  AS DEC
                        FROM enterpriseanalytics_edh_trnslt_ucan.P_JDE_ACCT_MSTR ACCT_MSTR
                        inner join enterpriseanalytics_edh_trnslt_ucan.P_JDE_GL_ACCT_BAL ACCT_BAL on ACCT_MSTR.acct_id=ACCT_BAL.acct_id
                        inner join enterpriseanalytics_edh_trnslt_ucan.P_JDE_BU_MSTR BU on  ACCT_BAL.bu_cd=BU.bu_cd
                        WHERE 
                        ACCT_BAL.FISCL_YR_NUM=%s
                        AND ACCT_MSTR.SCD_CUR_IND='Y'
                        AND ACCT_BAL.LDGR_TYP_CD='AA'
                        AND  ACCT_MSTR.CO_CD    =    '01001'
                        AND  ACCT_MSTR.OBJ_ACCT_CD    >=   '80000'
                        AND  ACCT_MSTR.OBJ_ACCT_CD   <=    '89999'
                        AND  BU.BU_CD    >=       '1001216800'
                        AND  BU.BU_CD    <=       '1001318703'
                        AND  ACCT_MSTR.OBJ_ACCT_CD     NOT IN  (  '70150',  '70202',  '70205',  '70208',  '70211',  '70217',  '70223',  '70230',  '70225',  '71003',  '71101',  '71102',  '71103',  '71104',  '71105',  '71111',  '71112',  '71202',  '71203',  '71301',  '71304',  '71305',  '71402',  '71403',  '71404',  '71601',  '71701',  '71702',  '72002',  '72101',  '72201',  '72202',  '72602',  '72603',  '73001',  '73201',  '73304',  '73310',  '73320',  '74004',  '74102',  '74105',  '74108',  '74111',  '74114',  '74126',  '75102',  '74202',  '75203',  '74130',  '75103',  '75209',  '75202',  '76003',  '76005',  '76102',  '77003',  '77072',  '77300',  '79152',  '80502',  '80503',  '80505',  '80506',  '80508',  '80511',  '80513',  '80514',  '80516',  '80831',  '81302',  '81303',  '81305',  '81306',  '81308',  '81311',  '81313',  '81314',  '81316',  '81336',  '81631',  '81701',  '82202',  '82204',  '82205',  '82206',  '82207',  '82211',  '82231',  '82232',  '82234',  '82235',  '82240',  '82262',  '82281',  '82285',  '82286',  '82287',  '82288',  '82301',  '82302',  '82303',  '82353',  '82373',  '82402',  '82403',  '82404',  '82405',  '82406',  '82408',  '82409',  '82461',  '82491',  '82521',  '82806',  '82851',  '82852',  '82921',  '82923',  '82926',  '82941',  '82961',  '82962',  '82963',  '83002',  '83008',  '83014',  '83022',  '83110',  '83132',  '83150',  '83153',  '83159',  '83453',  '83462',  '83464',  '84596',  '84682',  '84685',  '84731',  '84782',  '84783',  '84786',  '84787',  '85402',  '85508',  '85541',  '86102',  '86403')
                        AND  BU.BU_19_CTGRY_CD= '%s' 
                        and ACCT_MSTR.etl_updt_dtm >= (select * from max_table)
               ''') % (self.FISCL_YR_NUM,self.BU_CTGRY_CD)

        print(f"Here is the query :- {q}")
        query = self.spark.sql(q)
        query_count = query.count()
        print("here result :- ", query_count)
        if query_count > 0 :
            print("Conversion starts")
            doc = query.toJSON().map(lambda j: json.loads(j)).collect()
            # print("Json file result :- ", doc)
            self.es.create_index()
            self.es.insert_documents(doc, self.es_index_name,self.id_field)
            print(":::: Documents Loaded Successfully ::::: ")

    def execute(self):
        df_metadata = self.execute_query_p_jde_wo_lbr_abspn_rpt()
        # df_metadata.show()

        # self.query_metadata()

    def stop(self):
        # Stop the Spark session
        self.spark.stop()


def main():
    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME', 'secret_name', 'es_index_name','BU_CTGRY_CD','FISCL_YR_NUM','id_field'])
    executor = AthenaQueryExecutor(args)
    executor.execute()
    executor.stop()


# Usage
if __name__ == "__main__":
    main()