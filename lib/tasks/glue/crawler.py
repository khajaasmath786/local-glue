"""
Glue Crawler for running specified paths
"""
from datetime import datetime
import time
from botocore.exceptions import ClientError
import boto3


class GlueCrawlerManager:
    """
    Manages AWS Glue Crawler operations, including environment detection,
    partitioned folder validation, crawler creation, execution, and logging.
    """

    def __init__(self, logger):
        """
        Initialize GlueCrawlerManager with a logger.

        :param logger: Logger instance for logging.
        """
        self.logger = logger
        self.aws_environment = self.detect_aws_environment()

    def detect_aws_environment(self):
        """
        Detect and return the AWS environment (e.g., dev, test, prod).

        :return: AWS environment string.
        """
        sts_client = boto3.client("sts", region_name="us-east-2")
        account_id = sts_client.get_caller_identity()["Account"]
        account_mapping = {
            "143049391535": "dev",
            "203058073716": "test",
            "737965399985": "prod",
        }
        environment = account_mapping.get(account_id, "test")
        self.logger.info(f"Detected AWS environment: {environment}")
        return environment

    def get_default_glue_role(self):
        """
        Return the default Glue role based on the AWS environment.

        :return: Glue role string.
        """
        role_prefix = "baxaws"
        environment = self.aws_environment
        role_suffix = "enterpriseanalytics-edh-admin-role"
        return f"{role_prefix}-{environment}-{role_suffix}"

    def check_existing_table(
        self, glue_client, database_name, table_name
    ):
        """
        Check if the specified table exists in the Glue database.

        :param glue_client: Glue client instance.
        :param database_name: Database to check for the table.
        :param table_name: Name of the table to check.
        :return: True if the table exists, False otherwise.
        """
        self.logger.debug(
            f"Checking if table '{table_name}' exists in "
            f"database '{database_name}'"
        )
        try:
            glue_client.get_table(
                DatabaseName=database_name, Name=table_name
            )
            self.logger.info(
                f"Table '{table_name}' exists in database "
                f"'{database_name}'"
            )
            return True
        except glue_client.exceptions.EntityNotFoundException:
            self.logger.warning(
                f"Table '{table_name}' does not exist in "
                f"database '{database_name}'"
            )
            return False

    def create_and_run_crawler(
        self, database_name, table_prefix=None,
        s3_target_path=None, role=None, crawler_name=None
    ):
        """
        Create, update, or run an AWS Glue Crawler.

        :param database_name: Database for the crawler.
        :param table_prefix: Prefix for the table (optional).
        :param s3_target_path: S3 path for crawling.
        :param role: IAM role for Glue crawler.
        :param crawler_name: Glue crawler name.
        """
        if role is None:
            role = self.get_default_glue_role()
        if crawler_name is None:
            crawler_name = "baxaws-enterpriseanalytics-edh-sop-runner"

        if not s3_target_path:
            self.logger.error("S3 target path must be provided.")
            raise ValueError("S3 target path is required.")

        table_name = s3_target_path.rstrip("/").split("/")[-1]
        self.logger.info(f"Derived table_name from S3 path: {table_name}")

        glue_client = boto3.client("glue", region_name="us-east-2")

        try:
            glue_client.delete_crawler(Name=crawler_name)
            self.logger.info(
                f"Deleted existing crawler '{crawler_name}' "
                f"to update paths and prefix."
            )
            time.sleep(5)
        except glue_client.exceptions.EntityNotFoundException:
            self.logger.info(
                f"No existing crawler '{crawler_name}' found to delete."
            )

        try:
            self.logger.info(
                f"Creating new crawler '{crawler_name}' with updated "
                f"paths and prefix."
            )
            glue_client.create_crawler(
                Name=crawler_name,
                Role=role,
                DatabaseName=database_name,
                Targets={"S3Targets": [{"Path": s3_target_path}]},
                TablePrefix=table_prefix if table_prefix else "",
            )
        except ClientError as e:
            self.logger.error(f"Failed to create crawler: {e}")
            raise

        try:
            start_time = datetime.now()
            glue_client.start_crawler(Name=crawler_name)
            self.logger.info(
                f"Started crawler '{crawler_name}' for '{s3_target_path}'."
            )
            self.wait_for_crawler_to_finish(
                glue_client, crawler_name, database_name,
                table_name, start_time
            )
        except glue_client.exceptions.CrawlerRunningException:
            self.logger.warning(
                f"Crawler '{crawler_name}' is already running."
            )
        except ClientError as e:
            self.logger.error(f"Failed to start crawler: {e}")
            raise

    def wait_for_crawler_to_finish(
        self, glue_client, crawler_name, database_name,
        table_name, start_time
    ):
        """
        Wait until the crawler completes and log table changes.

        :param glue_client: Glue client instance.
        :param crawler_name: Name of the crawler.
        :param database_name: Database for the table.
        :param table_name: Name of the table to check.
        :param start_time: Crawler start time for elapsed time calculation.
        """
        self.logger.info(f"Waiting for crawler '{crawler_name}' to complete.")
        while True:
            response = glue_client.get_crawler_metrics(
                CrawlerNameList=[crawler_name]
            )
            metrics = response["CrawlerMetricsList"][0]
            if not metrics["StillEstimating"] and metrics[
                "TimeLeftSeconds"
            ] == 0:
                end_time = datetime.now()
                elapsed_time = end_time - start_time
                self.logger.info(
                    f"Crawler '{crawler_name}' completed in {elapsed_time}."
                )
                time.sleep(5)
                self.log_table_changes(
                    glue_client, database_name, table_name,
                    table_exists=True
                )
                break
            time.sleep(10)

    def log_table_changes(
        self, glue_client, database_name, table_name, table_exists
    ):
        """
        Log whether the table was created or updated after the crawler run.

        :param glue_client: Glue client instance.
        :param database_name: Database for the table.
        :param table_name: Name of the table to check.
        :param table_exists: Boolean indicating if the table existed
        before crawler run.
        """
        table_now_exists = self.check_existing_table(
            glue_client, database_name, table_name
        )

        if table_now_exists:
            if table_exists:
                self.logger.info(
                    f"Table '{table_name}' in database '{database_name}' "
                    f"was updated."
                )
            else:
                self.logger.info(
                    f"Table '{table_name}' in database '{database_name}' "
                    f"was newly created."
                )

            partitions = glue_client.get_partitions(
                DatabaseName=database_name, TableName=table_name
            )
            if partitions["Partitions"]:
                self.logger.info(
                    f" - Partitions for table '{table_name}':"
                )
                for partition in partitions["Partitions"]:
                    partition_values = partition["Values"]
                    self.logger.info(
                        f"   - Partition values: {partition_values}"
                    )
            else:
                self.logger.info(
                    f"   - No partitions found for table '{table_name}'."
                )
        else:
            self.logger.info(
                f"Table '{table_name}' was not created or updated in "
                f"database '{database_name}'."
            )
        self.logger.info("-" * 60)
