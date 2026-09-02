from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, from_unixtime, current_timestamp

class HudiTableQuery:
    """
    A class to query a Hudi table using Spark SQL and compute additional transformations
    such as commit timestamp conversion and lag calculations.
    """

    def __init__(self):
        """
        Initializes the Spark session and loads the Hudi table from a path.
        """
        self.spark = SparkSession.builder \
            .appName("HudiTableQuery") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")   

    def load_hudi_table(self):
        """
        Loads the Hudi table from the specified path.
        """
        hudi_table_path = 's3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/EUDTA/F4211'
        self.df = self.spark.read.format("hudi").load(hudi_table_path)
        # Register as a SQL view
        self.df.createOrReplaceTempView("hudi_table")
        print("Hudi table loaded and registered as 'hudi_table'")


    def run_query(self):
        """
        Executes the Spark SQL query to retrieve commit timestamp conversions and lags.
        """
        query = """
        SELECT
            _hoodie_commit_time,
            commit_timestamp,
            op,
            sddoco,
            sddcto,
            sdlitm,
            sdlnid,

            -- Convert the commit timestamp to CST (subtracting 6 hours)
            date_format(commit_timestamp - interval 6 hours, 'yyyy-MM-dd HH:mm:ss') AS commit_timestamp_cst,

            -- Convert the _hoodie_commit_time to CST
            date_format(from_unixtime(cast(_hoodie_commit_time as bigint) / 1000 - 6 * 3600), 'yyyy-MM-dd HH:mm:ss') AS hoodie_commit_time_cst,

            -- Calculate the lag (in seconds) between the current UTC time and the commit timestamp
            unix_timestamp(current_timestamp()) - unix_timestamp(commit_timestamp) AS lag_commit_timestamp_utc,

            -- Calculate the lag (in seconds) between the current UTC time and the _hoodie_commit_time
            unix_timestamp(current_timestamp()) - unix_timestamp(from_unixtime(cast(_hoodie_commit_time as bigint) / 1000)) AS lag_hoodie_commit_time_utc

        FROM enterpriseanalytics_edh_eudta.F4211
        ORDER BY _hoodie_commit_time DESC
        LIMIT 5
        """
        result_df = self.spark.sql(query)
        return result_df

    def display_results(self, result_df):
        """
        Displays the result of the Spark SQL query.
        :param result_df: DataFrame with the query results.
        """
        result_df.show(truncate=False)


# Example usage in Glue or Docker environment
if __name__ == "__main__":
    # Initialize the HudiTableQuery class
    hudi_query = HudiTableQuery()

    # Load the Hudi table
    # hudi_query.load_hudi_table()

    # Run the query
    result_df = hudi_query.run_query()

    # Display the results
    hudi_query.display_results(result_df)
