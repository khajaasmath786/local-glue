
"""
This class calls DB's to query and return records
"""
import boto3
from pyspark.sql import SparkSession

from lib.common.logger import Logger  # Import the Logger class


class DBConnector:
    """
    A class to handle database connections and queries using PySpark.
    """

    def __init__(self):
        """
        Initialize the logger for DBConnector.
        """
        self.logger = Logger(self.__class__.__name__)

    @staticmethod
    def get_connection_details(glue_client, glue_connection_name):
        """
        Retrieve connection details from AWS Glue.

        :param glue_client: Boto3 Glue client.
        :param glue_connection_name: Name of the Glue connection.
        :return: Tuple containing URL, username, password, 
                 and driver class.
        """
        logger = Logger(DBConnector.__name__)
        logger.info(f"Fetching connection details for: "
                    f"{glue_connection_name}")
        # Get the connection details
        connection = glue_client.get_connection(
            Name=glue_connection_name)['Connection']

        # Extract the connection properties
        connection_properties = connection['ConnectionProperties']
        logger.debug(f"Connection properties retrieved: "
                     f"{connection_properties}")

        url = connection_properties['JDBC_CONNECTION_URL']
        user = connection_properties['USERNAME']
        password = connection_properties['PASSWORD']  
        driver_class = connection_properties[
            'JDBC_DRIVER_CLASS_NAME']

        logger.info(f"Connection details retrieved: URL={url}, "
                    f"User={user}, Driver={driver_class}")
        return url, user, password, driver_class

    @staticmethod
    def query_db(spark, glue_client, glue_connection_name, query):
        """
        Execute a query on the specified database connection.

        :param spark: The Spark session.
        :param glue_client: Boto3 Glue client.
        :param glue_connection_name: Name of the Glue connection.
        :param query: SQL query to execute.
        :return: Spark DataFrame containing query results.
        """
        logger = Logger(DBConnector.__name__)
        logger.info(f"Executing query on DB connection: "
                    f"{glue_connection_name}")
        logger.debug(f"Query: {query}")
        # Retrieve connection details
        url, user, password, driver_class = DBConnector.get_connection_details(
            glue_client, glue_connection_name)

        df = spark.read.format("jdbc").options(
            url=url,
            user=user,
            password=password,
            query=query,
            driver=driver_class
        ).load()

        logger.info(f"Query executed successfully. Retrieved DataFrame "
                    f"with {df.count()} records.")
        return df


# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder.appName('GlueJob').getOrCreate()

    # Initialize boto3 client
    glue_client = boto3.client('glue', region_name='us-east-2')

    # Query SQL Server
    glue_connection_name = ("baxaws-enterpriseanalytics-edh-"
                            "baxterity-sql-server-connection")
    query = "select * from MasterSchedule.dbo.Prod21Plan"
    df = DBConnector.query_db(spark, glue_client, glue_connection_name, query)
    df.show()

    # Query Oracle
    glue_connection_name = ("baxaws-enterpriseanalytics-edh-"
                            "baxterity-gme-oracle-db-connection")
    query = "SELECT * from MUTDTA.F0911"
    df = DBConnector.query_db(spark, glue_client, glue_connection_name, query)
    df.show()

    # Query Oracle with specific query
    glue_connection_name = ("baxaws-enterpriseanalytics-edh-"
                            "baxterity-gsc-app-ff-oracle-db-connection")
    query = """SELECT FINCL_DT,
               OPRG_GRP2_DESCR AS FILL_LINE,
               round(sum(BO_PAST_DUE_ASP_CNTR_PRC)) BO_AT_NET_ASP,
               round(sum(AVG_SHIP_ASP_CNTR_PRC_USD_TRGT) * 0.25) AS 
               TARGET_BO_AT_NET,
               round(sum((AVG_BO_PAST_DUE_ASP_CNTR_PRC))/
               sum((AVG_SHIP_ASP_CNTR_PRC_USD_TRGT)),5) AS 
               DAILYBO_DAY_AVG_NET_SALES
               FROM GSC_APP_RPTGR.GLBL_BO_MTRC_SUMRY_ITEM_BP_VW 
               WHERE PLNT_DESCR_BO in ('NORTH COVE') 
               and AVG_SHIP_ASP_CNTR_PRC_USD_TRGT <> 0
               GROUP BY OPRG_GRP2_DESCR,FINCL_DT"""
    df = DBConnector.query_db(spark, glue_client, glue_connection_name, query)
    df.show()

    # Query PostgreSQL
    glue_connection_name = ("baxaws-enterpriseanalytics-edh-"
                            "postgresql")
    query = """SELECT ncrsinitiated, totalclosedncr, totalopenncr, 
               unreleasedbatches, firstpassyield, avgdaystorelease, 
               complaintincidentspermillion, loaddate, dateopened 
               FROM baxmfgdpl.frb_quality_data"""
    df = DBConnector.query_db(spark, glue_client, glue_connection_name, query)
    df.show()