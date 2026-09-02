import boto3
import logging


class S3FileArchiver:
    """
    A class to archive processed files in S3.
    """

    def __init__(self, processed_files):
        """
        Initializes the S3FileArchiver with S3 client and logger.
        Takes in a list of processed files to be archived.
        """
        self.s3_client = boto3.client('s3')
        self.logger = logging.getLogger('S3FileArchiver')
        logging.basicConfig(level=logging.INFO)
        self.processed_files = processed_files

    def archive_processed_files(self, archive_path: str):
        """
        Archives processed files to an S3 path.
        :param archive_path: S3 path for archiving (starts with s3://).
        """
        for file_path in self.processed_files:
            try:
                bucket_name, file_key = self._parse_s3_path(file_path)
                file_name = file_key.split("/")[-1]
                archive_key = f"{archive_path}/{file_name}"
                archive_key = archive_key.replace("s3://", "")
                archive_bucket, archive_file_key = \
                    self._parse_s3_path(f"s3://{archive_key}")
                self.logger.info(f"Archiving file: {file_key} "
                                 f"to {archive_key}")
                self.transfer_files(bucket_name, file_key, archive_bucket,
                                    archive_file_key)
            except Exception as e:  # pylint: disable=broad-except
                self.logger.error(f"Failed to archive {file_path}: {e}")

    def transfer_files(self, source_bucket, source_key,
                       target_bucket, target_key):
        """
        Transfer files from one S3 location to another.
        Verifies that the file exists in the archive
        with the correct size before deleting the original file.
        """
        try:
            source_obj = self.s3_client.head_object(
                Bucket=source_bucket, Key=source_key)
            source_size = source_obj['ContentLength']
            self.logger.info(f"Source file size: {source_size} bytes")
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.logger.info(f"Copying file from {source_bucket}/{source_key} "
                             f"to {target_bucket}/{target_key}")
            self.s3_client.copy_object(CopySource=copy_source,
                                       Bucket=target_bucket, Key=target_key)
            copied_obj = self.s3_client.head_object(
                Bucket=target_bucket, Key=target_key)
            copied_size = copied_obj['ContentLength']
            self.logger.info(f"Copied file size: {copied_size} bytes")

            if copied_size == source_size:
                self.logger.info(f"File copied to {target_bucket}/"
                                 f"{target_key} with correct size.")
            else:
                self.logger.error(f"Size mismatch: {source_size} vs "
                                  f"{copied_size} bytes.")
                return
            self.logger.info(f"Deleting original file from "
                             f"{source_bucket}/{source_key}")
            self.s3_client.delete_object(Bucket=source_bucket, Key=source_key)
            self.logger.info(f"Deleted original file {source_bucket}/"
                             f"{source_key}")
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error(f"Failed to transfer file: {e}")
            raise

    def _parse_s3_path(self, s3_path: str):
        """
        Parses the S3 path into bucket and key.
        :param s3_path: S3 path.
        :return: Bucket name and key.
        """
        path_parts = s3_path.replace("s3://", "").split("/", 1)
        return path_parts[0], path_parts[1]


if __name__ == '__main__':
    processed_files = [
        's3://baxaws-tst-enterpriseanalytics-edh-ita-inbound/'
        'GENSIGHT/global/globalscape/test/Table_statistics_1726747959112.csv'
    ]
    archive_path = 's3://baxaws-tst-enterpriseanalytics-edh-ita-inbound/' \
                   'GENSIGHT/archive'
    archiver = S3FileArchiver(processed_files)
    archiver.archive_processed_files(archive_path)
