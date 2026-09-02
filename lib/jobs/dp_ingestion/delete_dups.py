"""
This script manages Hudi table operations such as loading,
backing up, deleting records, and ensuring data integrity
in partitioned S3-based Hudi tables.
"""

import os
import optparse
from pyspark.sql import Window
from functools import reduce
from pyspark.sql.functions import col, row_number, when, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from lib.common.logger import Logger
from lib.common.constants import HudiConstants
import sys

logger = Logger("Hudi Data Manager")


class HudiDataManager:
    """
    Manages Hudi table operations including loading data,
    backing up records, deleting records, and ensuring data
    integrity in a partitioned table stored in S3.

    Attributes:
        config (dict): Configuration dictionary for Hudi.
        spark (SparkSession): The Spark session used for Hudi.
    """

    MAX_EXECUTORS = "max_executors"
    SCHEMA = "schema"
    TABLE_NAME = "table_name"

    EXEC_INSTANCES = "spark.executor.instances"
    DYN_MIN_EXEC = "spark.dynamicAllocation.minExecutors"
    DYN_MAX_EXEC = "spark.dynamicAllocation.maxExecutors"
    TIME_PARSER_POLICY = "spark.sql.legacy.timeParserPolicy"
    PARQUET_REBASE_READ = "spark.sql.parquet.datetimeRebaseModeInRead"
    AVRO_REBASE_WRITE = "spark.sql.avro.datetimeRebaseModeInWrite"
    PARQUET_WRITE_LEGACY = "spark.sql.parquet.writeLegacyFormat"


    def __init__(self, conf: dict) -> None:
        """
        Initialize the HudiDataManager with the provided config.

        Parameters:
            conf (dict): Configuration dictionary for Hudi. Must include:
                - max_executors
                - schema
                - table_name
                - bucket
                - sort_key
                - primary_key
                - upsert_parallelism
                - target
                - run_mode (optional, default="cluster")

        Returns:
            None

        Raises:
            ValueError: If conf is not a dict or missing required keys.
        """
        required_keys = [
            "max_executors", "schema", "table_name", "bucket",
            "sort_key", "primary_key", "upsert_parallelism", "target"
        ]
        if not isinstance(conf, dict):
            raise ValueError("Config must be a dictionary.")
        missing = [k for k in required_keys if k not in conf]
        if missing:
            raise ValueError(f"Missing required config keys: {missing}")
        self.config = conf
        
        self.schema = conf["schema"]
        self.table_name = conf["table_name"]
        self.primary_key = conf["primary_key"]
        self.sort_key = conf["sort_key"]
        self.bucket = conf["bucket"]
        self.target = conf["target"]
        self.max_executors = conf["max_executors"]
        self.upsert_parallelism= conf["upsert_parallelism"]
        self.partition_path = conf.get("partition_path", None)
        self.run_mode = conf.get("run_mode", "cluster")
        self.table_info = conf.get("table_info", {
            "schema": self.schema,
            "table_name": self.table_name,
            "primary_key": self.primary_key,
            "sort_key": self.sort_key,
            "partition_path": self.partition_path,
            "bucket": self.bucket,
            "target": self.target
        })
        self.spark = self._get_spark_session()
        

    def _get_spark_session(self) -> SparkSession:
        """
        Initialize and return a Spark session configured for Hudi operations.

        Returns:
            SparkSession: The configured Spark session.
        """
        logger.info("Initializing Spark session.")
        try:
            conf = SparkConf().set(self.EXEC_INSTANCES, "1") \
                            .set(self.DYN_MIN_EXEC, "1") \
                            .set(self.DYN_MAX_EXEC, self.max_executors) \
                            .set(self.TIME_PARSER_POLICY, "LEGACY") \
                            .set(self.PARQUET_REBASE_READ, "LEGACY") \
                            .set(self.AVRO_REBASE_WRITE, "LEGACY") \
                            .set(self.PARQUET_WRITE_LEGACY, "true")

            app_name = f"{self.schema}-{self.table_name}-CDC"
            spark = SparkSession.builder.enableHiveSupport() \
                                .appName(app_name) \
                                .config(conf=conf) \
                                .getOrCreate()

            spark.sparkContext.setLogLevel("ERROR")
            logger.info("Spark session initialized.")
            return spark
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            raise

    def _archive(self, df) -> None:
        """
        Backup records matching the deletion criteria as a Hudi table.

        Parameters:
            df (DataFrame): DataFrame containing records to backup.

        Returns:
            None

        Notes:
            - Uses class attributes for bucket, schema, and table_name.
            - Writes backup as a Hudi table to S3.

        Raises:
            Exception: If backup fails.
        """
        backup_path = (
            f"s3://{self.bucket}/raw-consolidated/"
            f"{self.schema.upper()}/"
            f"{self.table_name.upper()}_PART_BACKUP/"
        )
        logger.info(f"Backing up data to {backup_path} as Hudi table.")
        try:
            writer = df.write.format(HudiConstants.HUDI_FORMAT.value) \
                .option(HudiConstants.PRECOMBINE_FIELD_OPT_KEY.value, self.sort_key) \
                .option(HudiConstants.RECORDKEY_FIELD_OPT_KEY.value, self.primary_key) \
                .option(HudiConstants.TABLE_NAME.value, f"{self.table_name}_backup") \
                .option(HudiConstants.OPERATION_OPT_KEY.value, "insert") \
                .option(HudiConstants.UPSERT_PARALLELISM.value, self.upsert_parallelism) \
                .option(HudiConstants.HIVE_SYNC_ENABLED_OPT_KEY.value, HudiConstants.bolHiveSync.value) \
                .option(HudiConstants.HIVE_TABLE_OPT_KEY.value, f"{self.table_name}_backup") \
                .option(HudiConstants.HIVE_DATABASE_OPT_KEY.value, 
                        f"{HudiConstants.glue_db_prefix.value.lower()}{self.schema.lower()}")            
            if self.partition_path:
                writer = writer.option(HudiConstants.PARTITIONPATH_FIELD_OPT_KEY.value, self.partition_path)
            writer.mode("overwrite").save(backup_path)
            logger.info(f"Backup completed at {backup_path} as Hudi table.")
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            raise

    def _get_duplicates(self):
        """
        Identify duplicate records based on primary keys,
        keeping only the latest record (by _hoodie_commit_time)
        and marking older ones for deletion.

        Returns:
            DataFrame: DataFrame containing duplicate records to delete.
        """
        target_table = (
            f"{HudiConstants.glue_db_prefix.value.lower()}"
            f"{self.schema.lower()}."
            f"{self.table_name.lower()}"
        )
        pk_fields = [pk.strip() for pk in self.primary_key.split(",")]

        pk_select = ", ".join(pk_fields)
        dup_query = (
            f"SELECT * FROM {target_table} WHERE "
            f"({pk_select}) IN "
            f"(SELECT {pk_select} FROM {target_table} GROUP BY {pk_select} HAVING COUNT(*) > 1)"
        )
        all_dups_df = self.spark.sql(dup_query)
        logger.info(f"Total duplicate records before ranking: {all_dups_df.count()}")

        # Archive full duplicate set when not running locally (unchanged behavior)
        if self.run_mode != "local":
            self._archive(all_dups_df)

        # Rank within each PK group by latest commit time; add is_deleted for display
        window_spec = Window.partitionBy(*pk_fields).orderBy(col("_hoodie_commit_time").desc())
        ranked_df = (
            all_dups_df
            .withColumn("row_num", row_number().over(window_spec))
            .withColumn("is_deleted", when(col("row_num") > 1, lit("YES")).otherwise(lit("NO")))
        )

        # Rows to delete are those with rank > 1 (return only these; keep schema the same)
        dup_to_delete_df = ranked_df.filter(col("row_num") > 1).drop("row_num", "is_deleted")
        dup_count = dup_to_delete_df.count()

        if dup_count > 0:
            # Pick one composite PK from rows-to-delete as the sample key
            sample_row = dup_to_delete_df.select(*pk_fields).first()
            if sample_row is not None:
                sample_key = {pk: sample_row[pk] for pk in pk_fields}

                # Build AND filter over the full composite PK
                filter_expr = reduce(
                    lambda acc, pk: acc & (col(pk) == lit(sample_key[pk])),
                    pk_fields[1:],
                    (col(pk_fields[0]) == lit(sample_key[pk_fields[0]]))
                )

                key_str = ", ".join(f"{k}={sample_key[k]}" for k in pk_fields)
                logger.info(f"Showing sample records (kept & deleted) for [{key_str}]:")
                (
                    ranked_df
                    .filter(filter_expr)
                    .orderBy(col("row_num").asc())
                    .show(truncate=False)  # shows both row_num and is_deleted
                )

            logger.info(f"Found {dup_count} duplicate records to delete (keeping latest by _hoodie_commit_time).")
        else:
            logger.info("No duplicates found to delete.")

        return dup_to_delete_df


    def _delete(self, df) -> None:
        """
        Delete duplicate records from the Hudi table.

        Args:
            df (DataFrame): DataFrame containing duplicates.

        Returns:
            None

        Raises:
            Exception: If deletion fails.
        """
        logger.info(f"Deleting {df.count()} duplicate records.")
        try:
            writer = (
                df.write.format(HudiConstants.HUDI_FORMAT.value)
                .option(HudiConstants.PRECOMBINE_FIELD_OPT_KEY.value, self.sort_key)
                .option(HudiConstants.RECORDKEY_FIELD_OPT_KEY.value, self.primary_key)
                .option(HudiConstants.TABLE_NAME.value, self.table_name)
                .option(HudiConstants.OPERATION_OPT_KEY.value, HudiConstants.DELETE_OPERATION_OPT_VAL.value)
                .option(HudiConstants.UPSERT_PARALLELISM.value, self.upsert_parallelism)
                .option(HudiConstants.HIVE_SYNC_ENABLED_OPT_KEY.value, HudiConstants.bolHiveSync.value)
                .option(HudiConstants.HIVE_TABLE_OPT_KEY.value, self.table_name)
                .option(HudiConstants.HIVE_DATABASE_OPT_KEY.value, f"{HudiConstants.glue_db_prefix.value.lower()}{self.schema.lower()}")
            )
            if self.partition_path:
                writer = writer.option(HudiConstants.PARTITIONPATH_FIELD_OPT_KEY.value, self.partition_path)
            writer.mode("Append").save(self.target)
            logger.info("Records deleted successfully.")
        except Exception as e:
            logger.error(f"Failed to delete records: {e}")
            raise

    def _verify(self) -> None:
        """
        Verify the deletion process by checking for duplicate PK groups.

        Returns:
            None

        Raises:
            Exception: If verification fails or duplicates still exist.
        """
        target_table = (
            f"{HudiConstants.glue_db_prefix.value.lower()}"
            f"{self.schema.lower()}."
            f"{self.table_name.lower()}"
        )
        pk_fields = [pk.strip() for pk in self.primary_key.split(",")]
        pk_select = ", ".join(pk_fields)

        try:
            dup_cnt_q = (
                f"SELECT COUNT(1) AS dup_groups FROM ("
                f"  SELECT {pk_select} FROM {target_table} "
                f"  GROUP BY {pk_select} HAVING COUNT(*) > 1"
                f") t"
            )
            dup_groups = self.spark.sql(dup_cnt_q).collect()[0][0]

            if dup_groups > 0:
                logger.warning(f"Duplicate PK groups after deletion: {dup_groups}")
                self.spark.sql(
                    f"SELECT {pk_select} FROM {target_table} "
                    f"GROUP BY {pk_select} HAVING COUNT(*) > 1 LIMIT 10"
                ).show(truncate=False)
                raise RuntimeError("Verification failed: duplicates remain.")
            else:
                logger.info("Verification passed: no duplicate PK groups.")
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            raise


    def run(self) -> None:
        """
        Manage the Hudi data process, deleting duplicates 
        and ensuring integrity.

        Returns:
            None
        """
        logger.info("Starting Hudi data management process.")
        try:
            df = self._get_duplicates()
            if df.count() > 0:
                # Uncomment the next line to display duplicates for debugging
                df.show()
                sys.exit(1)
                self._archive(df)
                self._delete(df)
                self._verify()
            else:
                logger.info("No duplicates found for deletion.")
            logger.info("Hudi data management process completed.")
        except Exception as e:
            logger.error(f"Error during Hudi data management: {e}")
            raise
        finally:
            self.spark.stop()


def parse_arguments() -> dict:
    """
    Parse command line arguments using optparse.

    Returns:
        dict: Configuration dictionary for the app.
    """
    parser = optparse.OptionParser()
    parser.add_option("--sbucket", type=str, 
                      default="baxaws-tst-enterpriseanalytics-edh-jdeeu-inbound")
    parser.add_option("--sregion", type=str, default="eu-central-1")
    parser.add_option("-b", "--bucket", type=str, 
                      default="baxaws-tst-enterpriseanalytics-edh-jde-inbound")
    parser.add_option("-s", "--schema", type=str, default="EUDTA")
    parser.add_option("-t", "--table", type=str, default="f4311")
    parser.add_option("--primary_key", type=str, 
                      default="PDKCOO,PDDOCO,PDDCTO,PDSFXO,PDLNID")
    parser.add_option("--sort_key", type=str, default="change_seq")
    parser.add_option("--partition_path", type=str, default="partitionpath", 
                      help="Partition column name if table is partitioned, else None.")
    parser.add_option("-r", "--max_executors", type=str, default="5")
    parser.add_option("-p", "--bulk_insert_parallelism", type=int, default=6)
    parser.add_option("-u", "--upsert_parallelism", type=int, default=6)
    parser.add_option("--run_mode", type=str, default="cluster",
                      help="Execution mode: cluster (default) or local")
    parser.add_option("--local", action="store_true", default=False,
                      help="Use local default config for testing.")

    (options, args) = parser.parse_args()
    logger.info(f"Command-line arguments: {args}")

    # Default config for local testing (no sbucket/sregion here, they will be derived)
    local_defaults = {
        "bucket": "baxaws-tst-enterpriseanalytics-edh-jde-inbound",
        "schema": "EUDTA",
        "table_name": "f4311",
        "primary_key": "PDKCOO,PDDOCO,PDDCTO,PDSFXO,PDLNID",
        "sort_key": "change_seq",
        "partition_path": "partitionpath",
        "max_executors": "5",
        "bulk_insert_parallelism": 6,
        "upsert_parallelism": 6,
        "run_mode": "local",
        "target": os.path.join("s3://", "baxaws-tst-enterpriseanalytics-edh-jde-inbound", 
                               "raw-consolidated", 
                               "EUDTA", 
                               "f4311_PART")
    }

    config = {
        "sbucket": options.sbucket,
        "sregion": options.sregion,
        "bucket": options.bucket,
        "schema": options.schema,
        "table_name": options.table,
        "primary_key": options.primary_key,
        "sort_key": options.sort_key,
        "partition_path": options.partition_path,
        "max_executors": options.max_executors,
        "bulk_insert_parallelism": options.bulk_insert_parallelism,
        "upsert_parallelism": options.upsert_parallelism,
        "run_mode": options.run_mode,
        "target": os.path.join("s3://", options.bucket, 
                               "raw-consolidated", 
                               options.schema, 
                               options.table + '_PART')
    }

    # If --local is passed, override config
    if options.local:
        logger.info("Using local default config for testing.")
        config.update(local_defaults)

        # Restore user-provided schema/table
        config["schema"] = options.schema
        config["table_name"] = options.table

        # Derive sbucket/sregion only for local
        schema_upper = config["schema"].upper()
        if schema_upper == "APDTA":
            config["sbucket"] = "baxaws-tst-enterpriseanalytics-edh-jdeap-inbound"
            config["sregion"] = "ap-southeast-1"
        elif schema_upper == "EUDTA":
            config["sbucket"] = "baxaws-tst-enterpriseanalytics-edh-jdeeu-inbound"
            config["sregion"] = "eu-central-1"
        elif schema_upper in ("NADTA", "LADTA"):
            config["sbucket"] = "baxaws-tst-enterpriseanalytics-edh-jde-inbound"
            config["sregion"] = "us-east-1"

        # Recompute target after schema/table change

        config["target"] = os.path.join(
            "s3://", config["bucket"], "raw-consolidated",
            config["schema"],
            f"{config['table_name']}_PART" if config.get("partition_path") and str(config["partition_path"]).lower() != "none" else config["table_name"])

    # Add table_info bundle
    config["table_info"] = {
        "schema": config["schema"],
        "table_name": config["table_name"],
        "primary_key": config["primary_key"],
        "sort_key": config["sort_key"],
        "partition_path": config["partition_path"],
        "bucket": config["bucket"],
        "target": config["target"],
        "run_mode": config["run_mode"]
    }
    return config



if __name__ == '__main__':
    config = parse_arguments()
    manager = HudiDataManager(config)
    manager.run()
