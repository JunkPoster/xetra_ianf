"""
    File: test_s3.py
  Author: Ian Featherston
    Date: 2025-03-14
    Desc: Contains the unit tests for the S3BucketConnector class.

  Course: Udemy - "Writing Production-Ready ETL Pipelines in Python/Pandas" 
                - by Jan Schwarzlose
                - Section 5
"""

import os
import unittest

import boto3
from moto import mock_aws       # mock_s3 is deprecated. Use mock_aws instead

from xetra.common.s3 import S3BucketConnector


class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3BucketConnector Class
    """

    def setUp(self):
        """
        Setup the test environment
        """
        # Mocking S3 connection start
        self.mock_s3 = mock_aws()
        self.mock_s3.start()

        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'

        # Creating S3 Access Keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'

        # Creating a gucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3', 
                                 endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'eu-central-1'
                            })
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

        # Create a testing instance
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)

    def tearDown(self):
        """
        Tear down the test environment after the unit tests
        """
        # Mocking S3 connection stop
        self.mock_s3.stop()

    def test_list_files_in_prefix_ok(self):
        """
        Test the list_files_in_prefix method for getting 2 file keys
        as list on the mocked s3 bucket
        """
        # Expected reults
        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'

        # Test init
        csv_content = """col1,col2
        valA, valB"""
        self.s3_bucket.put_object(Key=key1_exp, Body=csv_content)
        self.s3_bucket.put_object(Key=key2_exp, Body=csv_content)

        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)

        # Tests after method execution
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)

        # Cleanup after tests
        self.s3_bucket.delete_objects(Delete={
            'Objects': [
                {
                    'Key': key1_exp
                },
                {
                    'Key': key2_exp
                }
            ]
        })

    def test_list_files_in_prefix_wrong_prefix(self):
        """
        Test the list_files_in_prefix method for wrong or nonexistent prefix
        """
        # Expected reults
        prefix_exp = 'no-prefix/'

        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)

        # Tests after method execution
        self.assertTrue(not list_result)


if __name__ == '__main__':
    #unittest.main()
    testIns = TestS3BucketConnectorMethods()
    testIns.setUp()
    testIns.test_list_files_in_prefix_ok()
    testIns.tearDown()
