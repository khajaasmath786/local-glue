"""
This script enables the execution of AWS Step Functions with specified
parameters. It supports configuring various aspects of the execution
environment, including S3 paths for input and output, cluster IDs, and more,
providing a flexible interface for data processing workflows.
It uses Boto3 for AWS interactions and logging for output.
"""
import json
import logging
import subprocess
import sys
import time

import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class StepFunctionExecutor:
    """
    Manages the execution of AWS Step Functions.

    This class handles the preparation and execution of a specific AWS Step
    Function based on the provided configuration parameters. It supports
    setting defaults,
    logging detailed parameter information, generating JSON input for the Step
    Function,and initiating the execution. Logging is extensively used to
    track the execution flow  and parameters.

    Attributes:
        cluster_id (str): The ID of the EMR cluster.
        input_path (str): S3 path for input data.
        target_path (str): S3 path where output data should be placed.
        sop_file_path (str): S3 path for the SOP file.
        temp_table_name (str): Name of the temporary table to be used.
        primary_keys (str): Primary keys for the data processing.
        partition_column (str): Column to partition the data on.
        state_machine_arn (str): ARN of the AWS Step Function to execute.
    """
    def __init__(self, **kwargs):
        """
        Initializes with parameters or defaults.
        :param kwargs: Keyword arguments for parameters.
        """
        self.session = None
        self.account_id = None
        self.profile_name = None
        # self.profile_name = kwargs.get('profile_name', 'saml')
        # uncomment above line for dev
        self.log_info_msg(self.profile_name)
        self.create_session()
        self.fetch_snowflake_secrets(secret_name="baxaws-dev-enterpriseanalytics-edh-edhq-snowflake-passphrase-secret")
        sys.exit(0)
        self.set_defaults()        
        self.__dict__.update(kwargs)
        self.log_parameters()

    def check_execution_status(self, execution_arn):
        """
        Continuously checks the status of the execution every 30 seconds.
        """
        client = self.session.client('stepfunctions', 'us-east-2')
        while True:
            response = client.describe_execution(executionArn=execution_arn)
            status = response.get('status')
            if status in ['SUCCEEDED', 'FAILED']:
                self.log_info_msg(f"Execution {status}.")
                if status == 'FAILED':
                    self.log_error_msg(f"Cause: {response.get('cause')}")
                break
            else:
                logging.info("Execution still in progress...")
                time.sleep(30)

    def set_account_id(self):
        """
        Retrieves the AWS account ID using STS get_caller_identity.
        """
        sts_client = self.session.client('sts')
        account_id = sts_client.get_caller_identity().get('Account')
        self.account_id = account_id

    def sync_s3_buckets(self, input_path, target_path):
        """
        Synchronizes two S3 paths using the AWS CLI 's3 sync'
        command.
        :param input_path: The S3 path to sync from.
        :param target_path: The S3 path to sync to.
        """
        try:
            self.log_info_msg(f"Profile {self.profile_name}")
            self.check_s3_path_exists(input_path)
            command = f"aws s3 sync {input_path} {target_path}" + \
                      (" --profile saml" if self.profile_name == 'saml'
                       else "")
            result = subprocess.run(command, check=True, shell=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            self.log_info_msg(result)
            msg = (f"S3 sync from {input_path} to {target_path} " +
                   "completed successfully.")
            self.log_info_msg(msg)
            self.check_s3_path_exists(target_path)
        except subprocess.CalledProcessError as e:
            error_message = e.stderr.decode('utf-8') if e.stderr \
                            else 'Unknown error'
            error_msg = "Failed to sync S3 buckets: " + error_message
            self.log_error_msg(error_msg)

    def create_session(self):
        """
        Creates a Boto3 session using the specified profile name.
        """
        if self.profile_name == 'saml':
            self.session = boto3.Session(profile_name=self.profile_name)
            self.set_account_id()
            return self.session
        else:
            self.session = boto3.Session()
            self.set_account_id()
            return self.session

    def fetch_snowflake_secrets(self, secret_name):
        """
        Fetches Snowflake credentials from AWS Secrets Manager and
        logs the details, excluding the password for security.
        :param secret_name: The name of the secret in Secrets Manager.
        """
        client = self.session.client('secretsmanager', 'us-east-2')
        self.log_info_msg(client)
        secret = None
        secret_dict = None
        try:
            get_secret_value_response = client.get_secret_value(
                                               SecretId=secret_name)
            secret = get_secret_value_response['SecretString']
            self.log_info_msg(secret)
            secret_dict = json.loads(secret)   
            self.log_info_msg(secret_dict)   
            sys.exit(0)           
        except Exception as e:
            self.log_info_msg(f"Error fetching secret: {e}")            
        # Log the details, cautiously excluding sensitive information like passwords
        self.log_info_msg("Successfully retrieved Snowflake secrets for:" +
                          f"{secret_name}")
        
        self.log_info_msg(f"Username: {secret_dict['username']}")
        self.log_info_msg(f"Account: {secret_dict['account']}")
        self.log_info_msg(f"Role: {secret_dict['role']}")
        self.log_info_msg(f"Schema: {secret_dict['schema']}")
        self.log_info_msg(f"Database: {secret_dict['database']}")
        self.log_info_msg(f"Warehouse: {secret_dict['warehouse']}")
        
        # Extract and return the secret details for further use
        sf_uid = secret_dict['username']
        sf_pwd = secret_dict['password']
        snowflake_account = secret_dict['account']
        snowflake_role = secret_dict['role']
        snowflake_schema = secret_dict['schema']
        snowflake_database = secret_dict['database']
        snowflake_warehouse = secret_dict['warehouse']

        # Configure Snowflake source options for reading/writing
        snowflake_source_name = "net.snowflake.spark.snowflake"
        self.log_info_msg(f"snowflake_source_name --> {snowflake_source_name}")

        sf_options = {
            "sfURL": f"{snowflake_account}.snowflakecomputing.com",
            "sfUser": sf_uid,
            "sfPassword": sf_pwd,
            "sfDatabase": snowflake_database,
            "sfSchema": snowflake_schema,
            "sfWarehouse": snowflake_warehouse,
            "sfRole": snowflake_role,
        }
        self.log_info_msg(f"sfOptions --> {sf_options}")
        return sf_options

    def set_defaults(self):
        """
        Sets default values for the parameters.
        """
        self.cluster_id = "j-1PUXYQW4PAUFR"
        # self.profile_name = None
        self.input_path = (
            "s3://baxaws-tst-enterpriseanalytics-edh-gabi-translate/"
            "APAC/supplychain/temp/salesorders/f_so_ln_2/"
        )
        self.target_path = (
            "s3://baxaws-tst-enterpriseanalytics-edh-gabi-translate/"
            "APAC/supplychain/temp/salesorders/f_so_ln_2_BACKUP/"
        )
        self.sop_file_path = (
            "s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/"
            "sop/p_jde_ar_rcpt_dtl_sop.txt"
        )
        self.temp_table_name = "temp"
        self.primary_keys = "so_ln_key"
        self.partition_column = ""
        self.state_machine_arn = (
            f"arn:aws:states:us-east-2:{self.account_id}:stateMachine:"
            "enterpriseanalytics-edh-dp-run-sop-stepfunction"
        )

    def log_info_msg(self, msg):
        """
        Logs a message.
        :param msg: message to print in logs.
        """
        logging.info(msg)

    def log_error_msg(self, msg):
        """
        Logs a message.
        :param msg: message to print in logs.
        """
        logging.info(msg)

    def log_parameters(self):
        """
        Logs the current parameters of the instance, each parameter separately.
        """
        self.log_info_msg(f"cluster_id={self.cluster_id}")
        self.log_info_msg(f"input_path={self.input_path}")
        self.log_info_msg(f"target_path={self.target_path}")
        self.log_info_msg(f"sop_file_path={self.sop_file_path}")
        self.log_info_msg(f"temp_table_name={self.temp_table_name}")
        self.log_info_msg(f"primary_keys={self.primary_keys}")
        self.log_info_msg(f"partition_column={self.partition_column}")

    def generate_input_json(self):
        """
        Generates the JSON input for AWS Step Functions execution.
        :return: A JSON string of input parameters.
        """
        input_dict = {
            "cluster_id": self.cluster_id,
            "input_path": self.input_path,
            "target_path": self.target_path,
            "sop_file_path": self.sop_file_path,
            "temp_table_name": self.temp_table_name,
            "primary_keys": self.primary_keys
        }
        if self.partition_column:
            input_dict["partition_column"] = self.partition_column
        return json.dumps(input_dict)

    def check_s3_path_exists(self, s3_path):
        """
        Checks if an S3 path exists and contains files.
        """
        if not s3_path.startswith("s3://"):
            msg = f"Invalid S3 path format: {s3_path}"
            self.log_error_msg(msg)
            return False
        s3_path_parts = s3_path[5:].split('/', 1)
        bucket = s3_path_parts[0]
        prefix = s3_path_parts[1] if len(s3_path_parts) > 1 else ''

        s3_client = self.session.client(
            's3', region_name='us-east-2'
        )

        try:
            resp = s3_client.list_objects_v2(
                Bucket=bucket, Prefix=prefix
            )
            if 'Contents' in resp:
                self.log_error_msg(f"files found in {bucket}/{prefix}")
                return True
            else:
                msg = f"No files found in {bucket}/{prefix}"
                self.log_error_msg(msg)
                return False
        except s3_client.exceptions.NoSuchBucket:
            msg = f"Bucket does not exist: {bucket}"
            self.log_error_msg(msg)
            return False
        except ClientError as e:
            msg = f"Error checking {bucket}/{prefix}: {str(e)}"
            self.log_error_msg(msg)
            return False

    def execute(self):
        """
        Starts the AWS Step Functions state machine execution.
        """
        self.sync_s3_buckets(self.input_path, self.target_path)
        response = self.start_execution()
        logging.info("Execution started: %s", response['executionArn'])
        self.check_execution_status(response['executionArn'])

    def start_execution(self):
        """
        Starts the AWS Step Functions state machine execution.
        :return: The response from the start-execution call.
        """
        client = self.session.client('stepfunctions', 'us-east-2')
        self.log_info_msg("Calling the Step Function")
        self.log_info_msg(f"state_machine_arn {self.state_machine_arn}")
        resp = client.start_execution(
            stateMachineArn=self.state_machine_arn,
            input=self.generate_input_json()
        )
        return resp


def parse_args(args):
    """
    Parses command-line arguments.
    :param args: List of command-line arguments.
    :return: Dictionary of parsed command-line arguments.
    """
    param_dict = {}
    for i in range(0, len(args), 2):
        key = args[i].lstrip('--')
        param_dict[key] = args[i + 1]
    return param_dict


if __name__ == "__main__":
    logging.info("Python Version: %s", {sys.version})
    logging.info("Python Executable: %s", {sys.executable})
    parsed_args = parse_args(sys.argv[1:])
    executor = StepFunctionExecutor(**parsed_args)
    executor.execute()
