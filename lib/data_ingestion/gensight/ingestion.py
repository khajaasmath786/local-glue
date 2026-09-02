import sys
import argparse
from awsglue.utils import getResolvedOptions
from lib.data_ingestion.common.transform import DataTransformer

class GensightDataProcessor:
    """
    Class to process Gensight data using a DataTransformer.
    """

    def __init__(self):
        """
        Initializes the GensightDataProcessor with necessary settings.
        """
        self.args = None
        self.env = 'glue'
        self.config_path = None
        self.s3_bucket = None
    
    
    def parse_arguments(self):
        """
        Parses arguments from AWS Glue or command line.
        """
        if '--env' in sys.argv:
            env_index = sys.argv.index('--env') + 1
            if env_index < len(sys.argv):
                self.env = sys.argv[env_index].lower()

        if self.env == 'local':
            self.parse_local_arguments()
        else:
            args_list = ['config_path', 'scripts_bucket','scripts_target_bucket']
            self.args = getResolvedOptions(sys.argv, args_list)

    def parse_local_arguments(self):
        """
        Parses arguments when running locally.
        """

        parser = argparse.ArgumentParser(description='Process arguments.')
        # parser.add_argument('--config_path', type=str, default='scripts/edh_pipelib/head-snapshot/dev/lib/data_ingestion/gensight/config/gensight-dev.yaml')
        # parser.add_argument('--config_path', type=str, default='scripts/edh_pipelib/head-snapshot/dev/lib/data_ingestion/gensight/config/country_launch_dates.yaml')
        parser.add_argument('--config_path', type=str, default='scripts/edh_pipelib/head-snapshot/dev/lib/data_ingestion/gensight/config/country_sales.yaml')
        parser.add_argument('--scripts_bucket', type=str, default='baxaws-dev-enterpriseanalytics-edh-ita-inbound')
        parser.add_argument('--scripts_target_bucket', type=str, default='baxaws-dev-enterpriseanalytics-edh-ita-inbound')
        parser.add_argument('--env', type=str, default='local')
        self.args = vars(parser.parse_args())

    def set_local_settings(self):
        """
        Sets default local settings if running locally.
        """
        self.config_path = self.args['config_path']
        self.s3_bucket = self.args['scripts_bucket']
        self.s3_target_bucket = self.args['scripts_target_bucket']

    def set_aws_glue_settings(self):
        """
        Sets settings if running in AWS Glue.
        """
        self.config_path = self.args['config_path']
        self.s3_bucket = self.args['scripts_bucket']
        self.s3_target_bucket = self.args['scripts_target_bucket']

    def execute(self):
        """
        Executes the data transformation process.
        """
        if self.env == 'local':
            self.set_local_settings()
        else:
            self.set_aws_glue_settings()

        transformer = DataTransformer(self.s3_bucket, self.config_path, 
                                      self.s3_target_bucket)
        transformer.execute(self.config_path)

def main():
    """
    Main function to execute the Gensight data processing.
    """
    processor = GensightDataProcessor()
    processor.parse_arguments()
    processor.execute()

if __name__ == "__main__":
    main()