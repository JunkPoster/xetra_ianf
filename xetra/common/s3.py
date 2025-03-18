"""
    File: s3.py
  Author: Ian Featherston
    Date: 2025-03-13
    Desc: Contains the S3BucketConnector class which is used to interact 
            with S3 buckets.

  Course: Udemy - "Writing Production-Ready ETL Pipelines in Python/Pandas" 
                - by Jan Schwarzlose
                - Section 5 & 6
"""
import os
import logging
from io import StringIO, BytesIO

import boto3
import pandas as pd

from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException


class S3BucketConnector:
    """
    Class for interacting with S3 buckets.
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url:str,
                 bucket: str, region_name: str = 'us-east-2'):
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
        self.session = boto3.Session(
            aws_access_key_id=os.environ[access_key],
            aws_secret_access_key=os.environ[secret_key],
            region_name=region_name
        )

        # The '_s3' is to signify that it's a protected/private variable
        self._s3 = self.session.resource(
            service_name='s3', 
            endpoint_url=endpoint_url
        )
        self._bucket = self._s3.Bucket(bucket)


    def list_files_in_prefix(self, prefix: str):
        """
        Lists all files containing a prefix in the S3 bucket.

        Parameters:
            prefix (str): Prefix to search for in the S3 bucket

        Returns:
            List of keys of the files containing the prefix
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files


    def read_csv_to_df(self, key: str, encoding: str = 'utf-8', sep: str = ','):
        """
        Reading a CSV file from an S3 Bucket into a DataFrame

        Parameters:
            key (str): Key of the file in the S3 bucket
            encoding (str): Encoding of the file
            sep (str): Separator of the file
        
        Returns:
            data_frame: Pandas DataFrame containing the CSV file's data
        """
        self._logger.info('Reading file %s/%s/%s',
                          self.endpoint_url, self._bucket.name, key)

        csv_obj = self._bucket.Object(key=key).get()\
                                .get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, sep=sep)

        return data_frame


    def write_df_to_s3(self, data_frame: pd.DataFrame, key: str, file_format: str):
        """
        Writes a Pandas DataFrame to S3.
        Supported formats: .csv, . parquet
        
        Parameters:
            data_frame (pd.DataFrame): DataFrame to write to S3
            key (str): Key of the file in the S3 bucket
            file_format (str): File format to write the DataFrame to
        """
        if data_frame.empty:
            self._logger.info('The dataframe is empty! No file will be written!')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            out_buffer.seek(0)  # Move the cursor to the beginning of the buffer
            # Write the data to S3
            return self.__put_object(out_buffer, key)

        self._logger.info('The file format %s is not supported to be written '
                          'to S3!', file_format)
        raise WrongFormatException


    def __put_object(self, out_buffer: StringIO | BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()

        Parameters:
            out_buffer (StringIO or BytesIO): Buffer containing the data
            key (str): Key of the file in the S3 bucket
        """
        self._logger.info('Writing file to %s/%s/%s',
                          self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
