"""
This script fetches EMR cluster details using AWS EMR API.
It gathers details of running EMR clusters, including 
instance information (ID and IP) for master, core, and 
task nodes. The results are collected, displayed in 
a Spark DataFrame, and exported to a CSV file.
"""

import time
import random
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql import Row


class EMRClusterDetails:
    """
    This class retrieves details of active EMR clusters and 
    their instances (master, core, and task nodes), including 
    instance IDs and IP addresses. The results can be displayed 
    in a Spark DataFrame or exported to a CSV file.
    """

    def __init__(self, appid='350668'):
        """
        Initialize the EMRClusterDetails class.

        :param appid: Application ID for filtering clusters.
        """
        self.appid = appid
        self.client = boto3.client('emr')
        self.spark = SparkSession.builder \
            .appName('EMRClusterInstances') \
            .getOrCreate()

    def get_instance_details(self, cluster_id, instance_group_type, retries=5):
        """
        Get instance details by instance group type (MASTER, CORE, TASK).
        Implements exponential backoff to handle throttling.
        
        :param cluster_id: The cluster's ID.
        :param instance_group_type: Type of instance group.
        :param retries: Number of retries in case of throttling.
        :return: A list of instances.
        """
        attempt = 0
        while attempt < retries:
            try:
                instances = self.client.list_instances(
                    ClusterId=cluster_id,
                    InstanceGroupTypes=[instance_group_type],
                    InstanceStates=['AWAITING_FULFILLMENT', 
                                    'PROVISIONING', 
                                    'BOOTSTRAPPING', 'RUNNING']
                )['Instances']
                return instances
            except ClientError as e:
                if e.response['Error']['Code'] == 'ThrottlingException':
                    sleep_time = min(2 ** attempt + random.random(), 60)
                    print(f"ThrottlingException occurred, retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                    attempt += 1
                else:
                    raise e
        raise Exception("Max retries exceeded for ListInstances API.")

    def get_clusters(self):
        """
        Fetch active clusters in STARTING, RUNNING, or 
        WAITING states.

        :return: A list of active clusters.
        """
        return self.client.list_clusters(
            ClusterStates=['STARTING', 'BOOTSTRAPPING', 
                           'RUNNING', 'WAITING']
        )['Clusters']

    def filter_and_collect_data(self):
        """
        Filter clusters based on the appid tag and collect 
        instance data for master, core, and task nodes.

        Filters clusters whose names contain "gabi", "bct", 
        or "consolidate".

        :return: A list of Spark Row objects containing cluster 
                 and instance details.
        """
        clusters = self.get_clusters()
        data = []

        for cluster in clusters:
            cluster_id = cluster['Id']
            cluster_name = cluster['Name'].lower()
            if any(keyword in cluster_name for keyword in ['gabi', 'bct', 'consolidate']):
                tags = self.client.describe_cluster(
                    ClusterId=cluster_id
                )['Cluster']['Tags']

                if any(tag['Key'] == 'Appid' and 
                       tag['Value'] == self.appid for tag in tags):

                    master_nodes = self.get_instance_details(
                        cluster_id, 'MASTER')
                    core_nodes = self.get_instance_details(
                        cluster_id, 'CORE')
                    task_nodes = self.get_instance_details(
                        cluster_id, 'TASK')

                    master_info = ", ".join(
                        [f"{node.get('Ec2InstanceId', 'N/A')} "
                         f"({node.get('PrivateIpAddress', 'N/A')})"
                         for node in master_nodes])
                    core_info = ", ".join(
                        [f"{node.get('Ec2InstanceId', 'N/A')} "
                         f"({node.get('PrivateIpAddress', 'N/A')})"
                         for node in core_nodes])
                    task_info = ", ".join(
                        [f"{node.get('Ec2InstanceId', 'N/A')} "
                         f"({node.get('PrivateIpAddress', 'N/A')})"
                         for node in task_nodes])

                    data.append(Row(ClusterName=cluster_name,
                                    ClusterId=cluster_id, 
                                    MasterNodes=master_info,
                                    CoreNodes=core_info, 
                                    TaskNodes=task_info))

        return data

    def create_dataframe(self):
        """
        Create a Spark DataFrame from the collected data.

        :return: A Spark DataFrame containing cluster and 
                 instance details.
        """
        data = self.filter_and_collect_data()
        return self.spark.createDataFrame(data)

    def export_to_csv(self, file_name="cluster_details.csv"):
        """
        Export the collected cluster details to a CSV file.

        :param file_name: The name of the CSV file to save.
        """
        df = self.create_dataframe()
        df.coalesce(1).write.mode('overwrite') \
            .option("header", "true") \
            .csv(file_name)

    def show_dataframe(self):
        """
        Display the Spark DataFrame containing the cluster and
        instance details.
        """
        df = self.create_dataframe()
        df.show(truncate=False)


if __name__ == "__main__":
    emr_details = EMRClusterDetails()
    emr_details.show_dataframe()
    emr_details.export_to_csv("cluster_details.csv")
