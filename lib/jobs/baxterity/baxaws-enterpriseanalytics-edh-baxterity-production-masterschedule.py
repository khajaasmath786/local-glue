import boto3
from pyspark.sql import SparkSession
from lib.clients.glue import DBConnector  # Import from the wheel file
import json
import logging
import sys
from awsglue.utils import getResolvedOptions
from lib.clients.elasticsearch_manager import ElasticSearchManager
from lib.utils.secrets import SecretsManager
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.exceptions import ClientError
from pyspark import SparkConf
from pyspark.context import SparkContext

class GlueJobRunner:
    """
    A class to run Glue job queries using DBConnector.
    """

    def __init__(self, args, region_name='us-east-2'):
        """
        Initialize the Glue job runner.

        :param region_name: AWS region name.
        """
        self.glue_client = boto3.client('glue', region_name=region_name)
        self.connector = DBConnector()
        self.JOB_NAME = args['JOB_NAME']
        self.secret_name = args['secret_name']
        self.es_index_name = args['es_index_name']
        self.id_field = args['id_field']
        #self.glue_connection_name = args['glue_connection_name']
        conf = SparkConf()
        self.sc = SparkContext(appName="SnowflakeIntegration", conf=conf)
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.spark.sparkContext.setLogLevel("ERROR")
        self.job = Job(self.glueContext)
        self.job.init(self.JOB_NAME, args)
        self.secret_manager = SecretsManager(args['secret_name'])
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
        select max(start_time) from baxmfgdpl.lth_job_audit where job_name = 'baxaws-enterpriseanalytics-edh-baxterity-production-masterschedule' and job_status = 'SUCCESS'
        """
        data = self.connector.query_db(
            self.spark, self.glue_client, glue_connection_name, query)
        print("here is the date :-", data.show())
        return  data


    def query_north_cove(self):
        
        #Calling query_metadata function for incremental laod
        df = self.query_metadata()
        latest_time_stamp = df.collect()[0]['max']
        print("This is the max result from postgres", latest_time_stamp)
        formatted_timestamp = latest_time_stamp.strftime('%Y-%m-%d')
        print("formatted_timestamp", formatted_timestamp)
        #formatted_timestamp = '2024-06-20'
        
        
        """
        Query SQL Server and return the DataFrame.
        """
        glue_connection_name = (
            "baxaws-enterpriseanalytics-edh-baxterity-sql-server-connection"
        )
        query = "SELECT OplanQuantity AS OPLANQUANTITY, CAST(PlanDate as DATE) AS PLANDATE, Prod21PlanId AS PROD21PLANID, BudgetQuantity AS BUDGETQUANTITY, FillingLine AS FILLINGLINE FROM MasterSchedule.dbo.Prod21Plan where PlanDate >= '%s'" % str(formatted_timestamp)
        print("Query for SQLserver", query)
        df = self.connector.query_db(
            self.spark, self.glue_client, glue_connection_name, query
        )
        #return df
        
        _count = df.count()
        print("Query result count :- ", _count)
        if _count == 0:
            print(" Received 0 count for this load, So skipping job execution")
            return None
            
        documents = df.toJSON().map(lambda j: json.loads(j)).collect()
        # es_index_name = "prod21plan"
        # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Here is the JSON", documents)
        self.es.create_index()
        self.es.insert_documents(documents, self.es_index_name,self.id_field)
        print("Data loaded into opensearch")
        return "Data loaded successfully"

    def run_all_queries(self):
        """
        Run all queries and display results.
        """
        df_north_cove = self.query_north_cove()
        

    def stop_spark(self):
        """
        Stop the Spark session.
        """
        self.spark.stop()


# Example usage
if __name__ == "__main__":
    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME','secret_name', 'es_index_name','id_field'])
    runner = GlueJobRunner(args)
    runner.run_all_queries()
    runner.stop_spark()
