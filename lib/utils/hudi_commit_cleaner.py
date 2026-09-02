import boto3
from datetime import datetime
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3FileHandler:
    """Handles S3 file operations with timestamp filtering."""

    def __init__(self, bucket_name, prefix):
        """
        Initialize the handler with S3 bucket and prefix.

        Args:
            bucket_name (str): Name of the S3 bucket.
            prefix (str): Prefix to search in the bucket.
        """
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name
        self.prefix = prefix

    def find_files_with_timestamp(
        self, timestamp_threshold, in_filename=True
    ):
        """
        Find files with timestamps greater than a threshold.

        Args:
            timestamp_threshold (str): Timestamp to compare.
            in_filename (bool): If True, compares filename timestamp;
                otherwise, compares S3 metadata LastModified.

        Returns:
            list: List of matching file paths.
        """
        matching_files = []
        paginator = self.s3.get_paginator('list_objects_v2')
        timestamp_dt = None

        if not in_filename:
            timestamp_dt = datetime.strptime(
                timestamp_threshold, "%Y-%m-%dT%H:%M:%S"
            )
        else:
            timestamp_pattern = re.compile(r'\d{14,}')

        for page in paginator.paginate(
            Bucket=self.bucket_name, Prefix=self.prefix
        ):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if in_filename:
                        match = timestamp_pattern.search(key)
                        if match and match.group() > timestamp_threshold:
                            matching_files.append(key)
                    else:
                        if obj['LastModified'] > timestamp_dt:
                            matching_files.append(key)
        return matching_files

    def delete_files(self, files_to_delete, perform_delete):
        """
        Delete specified files if perform_delete is True.

        Args:
            files_to_delete (list): List of file paths to delete.
            perform_delete (bool): If True, deletes the files.
        """
        if not perform_delete:
            print("Deletion is disabled. Skipping file deletions.")
            return

        for file_key in files_to_delete:
            self.s3.delete_object(Bucket=self.bucket_name, Key=file_key)
            print(f"Deleted: {file_key}")


# Usage Example
if __name__ == "__main__":
    bucket = "baxaws-prd-enterpriseanalytics-edh-jde-inbound"
    prefix = "raw-consolidated/LADTA/F4311_PART/"
    handler = S3FileHandler(bucket, prefix)

    # Find files based on filename timestamp
    timestamp = "20241121043852846"
    files = handler.find_files_with_timestamp(timestamp)

    if len(files) == 0:
        logger.info("No files found with timestamps greater than the threshold.")
    else:
        logger.info("Files with timestamps greater than the threshold:")
        for file in files:
            logger.info(file)

    # Delete files if the flag is set to True
    handler.delete_files(files, perform_delete=False)
