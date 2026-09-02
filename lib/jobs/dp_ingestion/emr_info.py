import boto3
import botocore
 
reg = "us-east-2"
# reg= "us-east-1"
# reg= "us-west-1"
# reg= "us-west-2"
# #Asia Pacific
# reg= "ap-south-1"
# reg= "ap-northeast-1"
# reg= "ap-northeast-2"
# reg= "ap-northeast-3"
# reg= "ap-southeast-1"
# reg= "ap-southeast-2"
# #Canada
# reg= "ca-central-1"
# #Europe
# reg= "eu-central-1"
# reg= "eu-west-1"
# reg= "eu-west-2"
# reg= "eu-west-3"
# reg= "eu-north-1"
# #South America
# reg= "sa-east-1"
 
def list_s3_buckets_with_edh(profile):
   session = boto3.Session(profile_name=profile)
   s3 = session.client('s3',region_name=reg)
   try:
       response = s3.list_buckets()
       buckets = [bucket['Name'] for bucket in response['Buckets'] if '-edh-' in bucket['Name']]
       if buckets:
           print("S3 Buckets with '-edh-':")
           for bucket in buckets:
               print(f" - {bucket}")
       else:
           print("No S3 buckets found with '-edh-'")
   except botocore.exceptions.ClientError as error:
       print(f"Error listing S3 buckets: {error}")
 
def list_ec2_instances_with_edh(profile):
   session = boto3.Session(profile_name=profile)
   ec2 = session.client('ec2',region_name=reg)
   try:
       response = ec2.describe_instances()
       instances = []
       for reservation in response['Reservations']:
           for instance in reservation['Instances']:
               for tag in instance.get('Tags', []):
                   if tag['Key'] == 'Name' and 'edh' in tag['Value']:
                       instances.append(instance['InstanceId'])
       if instances:
           print("EC2 Instances with 'edh':")
           for instance in instances:
               print(f" - {instance}")
       else:
           print("No EC2 instances found with '-edh-'")
   except botocore.exceptions.ClientError as error:
       print(f"Error listing EC2 instances: {error}")
 
def list_iam_users_with_edh(profile):
   session = boto3.Session(profile_name=profile)
   iam = session.client('iam',region_name=reg)
   try:
       response = iam.list_users()
       users = [user['UserName'] for user in response['Users'] if '-edh-' in user['UserName']]
       if users:
           print("IAM Users with '-edh-':")
           for user in users:
               print(f" - {user}")
       else:
           print("No IAM users found with '-edh-'")
   except botocore.exceptions.ClientError as error:
       print(f"Error listing IAM users: {error}")
 
def list_iam_roles_with_edh(profile):
   session = boto3.Session(profile_name=profile)
   iam = session.client('iam')
   try:
       # Initialize an empty list to store roles that contain 'edh'
       roles_with_edh = []
       # Paginate through all IAM roles
       paginator = iam.get_paginator('list_roles')
       for page in paginator.paginate():
           # Filter roles that contain 'edh' in the role name (case-insensitive)
           for role in page['Roles']:
               if '-edh-' in role['RoleName'].lower():
                   roles_with_edh.append(role['RoleName'])
       # Check if any roles were found
       if roles_with_edh:
           print("IAM Roles containing 'edh':")
           for role in roles_with_edh:
               print(f" - {role}")
       else:
           print("No IAM roles found with '-edh-'")
   except botocore.exceptions.ClientError as error:
       print(f"Error listing IAM roles: {error}")
 
def list_kms_aliases_with_edh(profile):
   session = boto3.Session(profile_name=profile)
   kms = session.client('kms',region_name=reg)
   paginator = kms.get_paginator('list_aliases')
   aliases = []
   try:
       #response = kms.list_aliases()
       for page in paginator.paginate():
        for alias in page['Aliases']:
            if "-edh-" in alias['AliasName']:
                aliases.append(alias['AliasName'])
       if aliases:
           print("KMS Aliases with '-edh-':")
           for alias in aliases:
               print(f" - {alias}")
       else:
           print("No KMS aliases found with '-edh-'")
   except botocore.exceptions.ClientError as error:
       print(f"Error listing KMS aliases: {error}")
 
def list_security_groups_with_edh(profile):
   session = boto3.Session(profile_name=profile)
   ec2 = session.client('ec2',region_name=reg)
   try:
       response = ec2.describe_security_groups()
       security_groups = [sg['GroupName'] for sg in response['SecurityGroups'] if '-edh-' in sg['GroupName']]
       if security_groups:
           print("Security Groups with '-edh-':")
           for sg in security_groups:
               print(f" - {sg}")
       else:
           print("No Security Groups found with '-edh-'")
   except botocore.exceptions.ClientError as error:
       print(f"Error listing Security Groups: {error}")
 
def list_emr_clusters_with_edh(profile):
   session = boto3.Session(profile_name=profile)
   emr = session.client('emr',region_name=reg)
   try:
       response = emr.list_clusters()
       clusters = [cluster['Id'] for cluster in response['Clusters'] if '-edh-' in cluster['Name']]
       if clusters:
           print("EMR Clusters with '-edh-':")
           for cluster in clusters:
               print(f" - {cluster}")
       else:
           print("No EMR clusters found with '-edh-'")
   except botocore.exceptions.ClientError as error:
       print(f"Error listing EMR clusters: {error}")
 
def main():
   profile = "saml"  # AWS profile to use
   print(reg)
   print(f"Checking for AWS resources containing '-edh-' using profile '{profile}':\n")
   list_s3_buckets_with_edh(profile)
   list_ec2_instances_with_edh(profile)
   list_iam_users_with_edh(profile)
   list_iam_roles_with_edh(profile)
   list_kms_aliases_with_edh(profile)
   list_security_groups_with_edh(profile)
   list_emr_clusters_with_edh(profile)
if __name__ == "__main__":
   main()