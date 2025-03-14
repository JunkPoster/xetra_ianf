"""
    File: s3.py
  Author: Ian Featherston
    Date: 2025-03-13
    Desc: Contains the S3BucketConnector class which is used to interact 
            with S3 buckets.

  Course: Udemy - "Writing Production-Ready ETL Pipelines in Python/Pandas" 
                - by Jan Schwarzlose
                - Section 5
"""

import os
import logging

import boto3


class S3BucketConnector():
    """
    Class for interacting with S3 buckets.
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url:str, 
                 bucket: str):
        """
        Constructor for S3BucketConnector

        Parameters:
            access_key (str): AWS Access Key
            secret_key (str): AWS Secret Key
            endpoint_url (str): Endpoint URL to S3
            bucket (str): S3 bucket name
        """
        self._logger = logging.getLogger(__name__)  # Initialize the logger

        # Assign our class endpoint_url
        self.endpoint_url = endpoint_url

        # Assign our AWS access key variables to the class
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key])

        # The '_s3' is to signify that it's a protected/private variable
        self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)


    def list_files_in_prefix(self, prefix: str):
        """
        Lists all files containing a prefix in the S3 bucket.

        Parameters:
            prefix (str): Prefix to search for in the S3 bucket

        Returns:
            List of keys of the files containing the prefix
        """
        return [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]


    def read_csv_to_df(self):
        """
        TODO
        """
        pass


    def write_df_to_csv(self):
        """
        TODO
        """
        pass
