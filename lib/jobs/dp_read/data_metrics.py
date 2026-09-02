"""
This script manages Hudi table operations such as
calculating lag metrics between commit timestamps
and _hoodie_commit_time and maintaining data integrity
in a non-partitioned table stored in S3.
"""

import os
import optparse
import datetime
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from lib.common.logger import Logger
from lib.common.constants import HudiConstants

logger = Logger("Hudi Metrics Manager")


class HudiMetricsManager:
    """
    Manages Hudi operations, including calculating lag
    metrics between commit timestamps and _hoodie_commit_time
    in non-partitioned S3-based Hudi tables.
    """

    def __init__(self, conf):
        """
        Initialize the HudiMetricsManager with provided config.

        Args:
            conf (dict): Configuration dictionary for Hudi operations.
        """
        self.config = conf
        self.spark = self._get_spark_session()
        self.current_year = str(datetime.datetime.now().year)

    def _get_spark_session(self):
        """
        Initialize and return a Spark session configured for Hudi.

        Returns:
            SparkSession: The configured Spark session.
        """
        logger.info("Initializing Spark session.")
        conf = (SparkConf()
                .set("spark.executor.instances", "5")
                .set("spark.dynamicAllocation.enabled", "true")
                .set("spark.dynamicAllocation.minExecutors", "5")
                .set("spark.dynamicAllocation.maxExecutors", "20")
                .set("spark.dynamicAllocation.initialExecutors", "5")
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "8g")
                .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
                .set("spark.sql.avro.datetimeRebaseModeInWrite", "LEGACY")
                .set("spark.sql.parquet.writeLegacyFormat", "true"))

        app_name = f"{self.config['schema']}-" \
                   f"{self.config['table_name']}-Metrics"
        spark = (SparkSession.builder
                 .enableHiveSupport()
                 .appName(app_name)
                 .config(conf=conf)
                 .getOrCreate())

        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session initialized.")
        return spark

    def calculate_lag_metrics(self):
        """
        Calculate lag metrics between commit_timestamp and
        _hoodie_commit_time.

        Returns:
            DataFrame: A DataFrame containing the calculated lag metrics.
        """
        logger.info(f"Calculating lag metrics for "
                    f"{self.config['table_name']}")

        table_name = (f"{HudiConstants.glue_db_prefix.value.lower()}"
                      f"{self.config['schema'].lower()}."
                      f"{self.config['table_name'].lower()}")

        df = self.spark.sql(f"""
                            SELECT * FROM {table_name}
                            WHERE _hoodie_partition_path='{self.current_year}'
                            ORDER BY cast(_hoodie_commit_time AS bigint) DESC
                            """)

        df_with_lags = (df
                        .withColumn(
                            "hoodie_commit_time_cst",
                            F.when(F.length("_hoodie_commit_time") == 17,
                                   F.date_format(
                                       F.date_add(
                                           F.col("_hoodie_commit_time")
                                           .substr(1, 14)
                                           .cast("timestamp"), -6),
                                       "yyyy-MM-dd HH:mm:ss"))
                            .otherwise(None))
                        .withColumn(
                            "commit_timestamp_cst",
                            F.date_format(
                                F.date_add(
                                    F.col("commit_timestamp")
                                    .cast("timestamp"), -6),
                                "yyyy-MM-dd HH:mm:ss"))
                        .withColumn(
                            "lag_commit_to_hoodie_seconds",
                            F.when(F.length("_hoodie_commit_time") == 17,
                                   F.unix_timestamp(
                                       F.col("_hoodie_commit_time")
                                       .substr(1, 14), 'yyyyMMddHHmmss') -
                                   F.unix_timestamp(
                                       F.col("commit_timestamp")
                                       .cast("timestamp"))))
                        .withColumn(
                            "lag_commit_to_hoodie_minutes",
                            F.col("lag_commit_to_hoodie_seconds") / 60)
                        .withColumn(
                            "lag_commit_to_hoodie_hours",
                            F.col("lag_commit_to_hoodie_seconds") / 3600))

        window_spec = Window.orderBy(
            F.col("_hoodie_commit_time").cast("bigint").desc())

        df_final = (df_with_lags
                    .withColumn("sequence_number",
                                F.row_number().over(window_spec))
                    .filter(F.col("sequence_number") <= 10)
                    .withColumn(
                        "primary_key", F.concat(
                            F.lit(self.config['table_name']),
                            F.lit("_"),
                            F.col("sequence_number")))
                    .select(
                        "primary_key",
                        "sequence_number",
                        "hoodie_commit_time_cst",
                        "commit_timestamp_cst",
                        "lag_commit_to_hoodie_seconds",
                        "lag_commit_to_hoodie_minutes",
                        "lag_commit_to_hoodie_hours",
                        "*")
                    .orderBy(F.col("_hoodie_commit_time")
                             .cast("bigint").desc()))

        df_final.show(truncate=False)
        logger.info("Lag metrics calculation completed.")
        return df_final

    def write_metrics_to_table(self, df):
        """
        Write the calculated metrics to a new non-partitioned
        table in Glue Catalog.

        Args:
            df (DataFrame): The DataFrame containing the calculated metrics.
        """
        table_name = f"{self.config['table_name']}_metrics_summary"
        target_path = self.config["target_path"]

        logger.info(f"Writing metrics to table {table_name} "
                    f"at {target_path}")
        df.write.format(HudiConstants.HUDI_FORMAT.value) \
            .option(HudiConstants.PRECOMBINE_FIELD_OPT_KEY.value,
                    "commit_timestamp") \
            .option(HudiConstants.RECORDKEY_FIELD_OPT_KEY.value,
                    "primary_key") \
            .option(HudiConstants.TABLE_NAME.value, table_name) \
            .option(HudiConstants.HIVE_SYNC_ENABLED_OPT_KEY.value, "true") \
            .option(HudiConstants.HIVE_TABLE_OPT_KEY.value, table_name) \
            .option(HudiConstants.HIVE_DATABASE_OPT_KEY.value,
                    f"{HudiConstants.glue_db_prefix.value.lower()}"
                    f"{self.config['schema'].lower()}") \
            .mode("Append").save(target_path)

        logger.info(f"Metrics written to {table_name} and synced "
                    f"to Glue Catalog.")

    def manage(self):
        """
        Manage the Hudi data process, calculating lag metrics,
        and writing them to a non-partitioned Glue table.
        """
        logger.info("Starting Hudi data management process.")
        df = self.calculate_lag_metrics()
        self.write_metrics_to_table(df)
        logger.info("Hudi data management process completed.")


def parse_arguments():
    """
    Parse command line arguments using optparse.

    Returns:
        dict: Configuration dictionary for the app.
    """
    parser = optparse.OptionParser()
    parser.add_option("--sbucket", type=str,
                      default="baxaws-prd-enterpriseanalytics-edh-jdeeu-inbound")
    parser.add_option("--sregion", type=str, default="eu-central-1")
    parser.add_option("-b", "--bucket", type=str,
                      default="baxaws-prd-enterpriseanalytics-edh-jde-inbound")
    parser.add_option("-s", "--schema", type=str, default="EUDTA")
    parser.add_option("-t", "--table", type=str, default="f4311")
    parser.add_option("--primary_key", type=str,
                      default="PDKCOO,PDDOCO,PDDCTO,PDSFXO,PDLNID")
    parser.add_option("--sort_key", type=str, default="change_seq")
    parser.add_option("-r", "--max_executors", type=str, default="5")
    parser.add_option("-p", "--bulk_insert_parallelism", type=int,
                      default=6)
    parser.add_option("-u", "--upsert_parallelism", type=int,
                      default=6)

    (options, args) = parser.parse_args()
    logger.info(f"Command-line arguments: {args}")

    conf = {
        "sbucket": options.sbucket,
        "sregion": options.sregion,
        "bucket": options.bucket,
        "schema": options.schema,
        "table_name": options.table,
        "primary_key": options.primary_key,
        "sort_key": options.sort_key,
        "max_executors": options.max_executors,
        "bulk_insert_parallelism": options.bulk_insert_parallelism,
        "upsert_parallelism": options.upsert_parallelism,
        "target_path": os.path.join(
            "s3://", options.bucket,
            "raw-consolidated/metrics-data/edh_metrics_summary")
    }
    return conf


if __name__ == '__main__':
    config = parse_arguments()
    manager = HudiMetricsManager(config)
    manager.manage()
