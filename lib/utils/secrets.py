"""
This script contains the SecretsManager class for retrieving
secrets from AWS Secrets Manager.
"""
import base64
import json
import sys

import boto3
from botocore.exceptions import ClientError

from lib.common.logger import Logger


class SecretsManager:
    """
    Class handles secret retrieval from AWS Secrets Manager.
    """
    def __init__(self, secret_name, region_name="us-east-2",
                 aws_profile="default"):
        """
        Initializes the SecretsManager with AWS configuration.
        Args:
            secret_name (str): Name of the secret.
            region_name (str): AWS region of the secrets.
            aws_profile (str): AWS profile name for credentials.
        """
        self.secret_name = secret_name
        self.region_name = region_name
        # self.session = boto3.Session(profile_name=aws_profile)
        self.session = boto3.Session()
        self.client = self.session.client(
            service_name='secretsmanager', 
            region_name=region_name
        )
        self.glue_client = self.session.client('glue', region_name=region_name)
        self.logger = Logger(self.__class__.__name__)

    def get_secret(self, name=None):
        """
        Retrieves secret from AWS Secrets Manager.
        Returns:
            str: The retrieved secret as a string.
        Raises:
            ClientError: Error from AWS Secrets Manager.
        """
        try:
            response = self.client.get_secret_value(
                SecretId=self.secret_name
            )
        except ClientError as e:
            error_msg = f"Failed to retrieve secret {self.secret_name}: {e}"
            self.logger.error(error_msg)
            raise e
        else:
            if 'SecretString' in response:
                return response['SecretString']
            else:
                decoded = base64.b64decode(response['SecretBinary'])
                return decoded.decode('utf-8')

    def fetch_snowflake_secrets(self):
        """
        Fetches Snowflake credentials from AWS Secrets Manager.
        Returns:
            dict: Snowflake credentials.
        Raises:
            SystemExit: Exits if secrets cannot be retrieved.
        """
        try:
            self.logger.info(self.secret_name)
            response = self.client.get_secret_value(
                SecretId=self.secret_name
            )
            if 'SecretString' in response:
                secret_dict = json.loads(response['SecretString'])
            else:
                decoded = base64.b64decode(response['SecretBinary'])
                secret_dict = json.loads(decoded.decode('utf-8'))

            sf_options = {
                "sfURL": secret_dict.get('url', ''),
                "sfUser": secret_dict.get('username', ''),
                "sfPassword": secret_dict.get('password', ''),
                "sfDatabase": secret_dict.get('database', ''),
                "sfSchema": secret_dict.get('schema', ''),
                "sfWarehouse": secret_dict.get('warehouse', ''),
                "sfRole": secret_dict.get('role', '')
            }
            return sf_options
        except ClientError as e:
            self.logger.error(
                f"Failed to retrieve Snowflake credentials: {e}"
            )
            sys.exit(1)

    def fetch_baxterity_aws_secrets(self):
        """
        Fetches Baxterity AWS credentials from AWS Secrets Manager ES.
        Returns:
            dict: credentials.
        Raises:
            SystemExit: Exits if secrets cannot be retrieved.
        """
        try:
            self.logger.info(self.secret_name)
            response = self.client.get_secret_value(
                SecretId=self.secret_name
            )
            if 'SecretString' in response:
                secret_dict = json.loads(response['SecretString'])
            else:
                decoded = base64.b64decode(response['SecretBinary'])
                secret_dict = json.loads(decoded.decode('utf-8'))
            options = {
                "access_key": secret_dict.get('accesskey', ''),
                "secret_key": secret_dict.get('secreykey', ''),
                "es_endpoint": secret_dict.get('endpoint', ''),
                "es_port": secret_dict.get('port', '443'),

            }
            
            return options
        except ClientError as e:
            self.logger.error(
                f"Failed to retrieve Baxterity AWS credentials: {e}"
            )
            sys.exit(1)

    def get_connection_details_and_secret(self, connection_name):
        """
        Retrieves connection details from AWS Glue and the corresponding secret from
        AWS Secrets Manager, then prints JDBC URL, username, and password.
        Args:
            connection_name (str): The name of the AWS Glue connection.
        """
        try:
            # Fetch connection properties from AWS Glue
            connection_response = self.glue_client.get_connection(Name=connection_name)
            connection_properties = connection_response['Connection']['ConnectionProperties']
            self.logger.info(f"Connection details for {connection_name} retrieved.")
            
            # Fetch the secret using the SECRET_ID from the connection properties
            secret_id = connection_properties.get('SECRET_ID')
            if secret_id:
                secret = self.get_secret(secret_id)
                if isinstance(secret, str):
                    secret = json.loads(secret)  # Ensure it's a dictionary

                # Extract details from the secret
                jdbc_url = connection_properties.get('JDBC_CONNECTION_URL')
                username = secret.get('username')
                # password = secret.get('password')  # Handle with care

                # Securely log or print these details
                self.logger.info(f"JDBC URL: {jdbc_url}")
                self.logger.info(f"Username: {username}")
                # self.logger.info(f"Password: {password}")  # Uncomment cautiously

        except ClientError as e:
            error_msg = f"Failed to retrieve connection or secret details: {e}"
            self.logger.error(error_msg)
            raise e



def main():
    """
    Main function to test the SecretsManager.
    """
    secret_name = "baxaws-dev-enterpriseanalytics-edh-edhq-snowflake-passphrase-secret"
    connection_name = "baxaws-enterpriseanalytics-edh-gsc-app-ff-oracle-connection"
    aws_profile = "saml"
    region_name = "us-east-2"
    secrets_manager = SecretsManager(secret_name, region_name, aws_profile)

    try:
        credentials = secrets_manager.fetch_snowflake_secrets()
        print("Snowflake Credentials:", credentials)
        secrets_manager.get_connection_details_and_secret(connection_name=connection_name)
    except Exception as e:
        print("An error occurred:", e)
    
    
    bax_secret_name = "baxaws-dev-enterpriseanalytics-edh-baxterity-aws-secrets"
    bax_secret_name ="baxaws-enterpriseanalytics-edh-baxterity-opensearch-credentials"
    bax_secrets_manager = SecretsManager(bax_secret_name)

    try:
        credentials = bax_secrets_manager.fetch_baxterity_aws_secrets()
        print("fetch_baxterity_aws_secrets Credentials:", credentials)        
    except Exception as e:
        print("An error occurred:", e)


if __name__ == "__main__":
    main()
