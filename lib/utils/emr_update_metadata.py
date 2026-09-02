"""
This script enforces IMDSv2 on all nodes (master, core, 
task) of an existing EMR cluster using Boto3. The script 
also verifies if IMDSv2 is successfully applied.

Equivalent AWS CLI command to enforce IMDSv2:
aws ec2 modify-instance-metadata-options \
  --instance-id i-xxxxxxxxxxxxxxxxx \
  --http-tokens required \
  --http-endpoint enabled \
  --region your-region

To verify if IMDSv2 is enforced:
aws ec2 describe-instances \
  --instance-ids i-xxxxxxxxxxxxxxxxx \
  --query "Reservations[*].Instances[*].MetadataOptions" \
  --region your-region

Check the output for:
HttpTokens: required
HttpEndpoint: enabled
"""

import boto3
import argparse
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EMRMetadataEnforcer:
    def __init__(self, cluster_id, region):
        """
        Initializes the EMRMetadataEnforcer class.

        @param cluster_id: The ID of the EMR cluster.
        @param region: The AWS region where the cluster is located.
        """
        self.cluster_id = cluster_id
        self.region = region
        self.emr_client = boto3.client('emr', region_name=region)
        self.ec2_client = boto3.client('ec2', region_name=region)

    def get_instance_ids(self):
        """
        Retrieves the EC2 instance IDs for all nodes 
        (master, core, and task) in the EMR cluster.

        @return: A list of EC2 instance IDs.
        """
        response = self.emr_client.list_instances(
            ClusterId=self.cluster_id,
            InstanceGroupTypes=['MASTER', 'CORE', 'TASK']
        )
        return [instance['InstanceId'] 
                for instance in response['Instances']]

    def enforce_imdsv2(self, instance_id):
        """
        Enforces IMDSv2 on a specific EC2 instance by modifying 
        its metadata options.

        @param instance_id: The EC2 instance ID where IMDSv2 
                            will be enforced.
        """
        self.ec2_client.modify_instance_metadata_options(
            InstanceId=instance_id,
            HttpTokens='required',
            HttpEndpoint='enabled'
        )
        logger.info(f"Enforced IMDSv2 on {instance_id}")

    def verify_imdsv2(self, instance_id):
        """
        Verifies whether IMDSv2 is enforced on the given instance.

        @param instance_id: The EC2 instance ID to verify IMDSv2.

        @return: True if IMDSv2 is enforced, otherwise False.
        """
        response = self.ec2_client.describe_instances(
            InstanceIds=[instance_id]
        )
        options = response['Reservations'][0]\
                  ['Instances'][0]['MetadataOptions']
        return (options['HttpTokens'] == 'required' and 
                options['HttpEndpoint'] == 'enabled')

    def apply_imdsv2_to_all(self):
        """
        Applies IMDSv2 enforcement and verification on all 
        nodes in the EMR cluster.
        """
        instance_ids = self.get_instance_ids()
        for instance_id in instance_ids:
            self.enforce_imdsv2(instance_id)
            if self.verify_imdsv2(instance_id):
                print(f"IMDSv2 enforced on {instance_id}")
                logger.info(f"Verified IMDSv2 on {instance_id}")
            else:
                logger.error(f"IMDSv2 not enforced on {instance_id}")


def run_enforcement(cluster_id, region):
    """
    Initializes the EMRMetadataEnforcer and applies IMDSv2 
    to all instances in the specified EMR cluster.

    @param cluster_id: The EMR cluster ID.
    @param region: The AWS region where the cluster is located.
    """
    enforcer = EMRMetadataEnforcer(cluster_id, region)
    enforcer.apply_imdsv2_to_all()


def main():
    """
    Main function that parses command-line arguments and 
    invokes the enforcement process.
    """
    parser = argparse.ArgumentParser(
        description="Enforce IMDSv2 on EMR cluster nodes")
    parser.add_argument('--cluster-id', required=True,
                        help="The EMR cluster ID")
    parser.add_argument('--region', default="us-east-1",
                        help="The AWS region")

    args = parser.parse_args()

    run_enforcement(args.cluster_id, args.region)


if __name__ == "__main__":
    main()
