"""
This script defines the SparkSessionManager class
for managing Spark sessions and distributing files
across nodes in a cluster.
"""
from pyspark.sql import SparkSession

class SparkSessionManager:
    def __init__(self, app_name):
        """
        Initialize the Spark session with the given app name.

        :param app_name: Name of the Spark application.
        """
        # Add the configuration option here
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
            .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .getOrCreate()

    def add_file(self, file_path):
        """
        Add a file to be distributed to all nodes.

        :param file_path: Path of the file to add.
        """
        self.spark.sparkContext.addFile(file_path)

    def get_spark(self):
        """
        Get the Spark session.

        :return: Spark session object.
        """
        return self.spark

    def stop(self):
        """Stop the Spark session."""
        self.spark.stop()