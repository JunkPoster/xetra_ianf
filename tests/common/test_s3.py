"""
    File: test_s3.py
  Author: Ian Featherston
    Date: 2025-03-14
    Desc: Contains the unit tests for the S3BucketConnector class.

  Course: Udemy - "Writing Production-Ready ETL Pipelines in Python/Pandas" 
                - by Jan Schwarzlose
                - Section 5 & 6
"""
import os
import unittest
from io import StringIO, BytesIO

import boto3
import pandas as pd
from moto import mock_aws       # mock_s3 is deprecated. Use mock_aws instead

from xetra.common.s3 import S3BucketConnector
from xetra.common.custom_exceptions import WrongFormatException


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

        # Creating a bucket on the mocked s3
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
        # Expected results
        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'

        # Test init
        csv_content = """col1,col2
        valA, valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)

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
        # Expected results
        prefix_exp = 'no-prefix/'

        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)

        # Tests after method execution
        self.assertTrue(not list_result)


    def test_read_csv_to_df_ok(self):
        """
        Tests the read_csv_to_df method for reading one .csv file from
        the mocked S3 bucket
        """
        # Expected Results
        key_exp = 'test.csv'
        col1_exp = 'col1'
        col2_exp = 'col2'
        val1_exp = 'val1'
        val2_exp = 'val2'
        log_exp = f'Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'

        # Test init
        csv_content = f'{col1_exp},{col2_exp}\n{val1_exp},{val2_exp}'
        self.s3_bucket.put_object(Body=csv_content, Key=key_exp)

        # Method execution
        with self.assertLogs() as logm:
            df_result = self.s3_bucket_conn.read_csv_to_df(key_exp)
            # Log test after method execution
            self.assertIn(log_exp, logm.output[0])

        # Tests after method execution
        self.assertEqual(df_result.shape[0], 1)
        self.assertEqual(df_result.shape[1], 2)
        self.assertEqual(val1_exp, df_result[col1_exp][0])
        self.assertEqual(val2_exp, df_result[col2_exp][0])

        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_empty(self):
        """
        Tests the write_df_to_s3() method with an empty DataFrame as input
        """
        # Expected Results
        return_exp = None
        log_exp = 'The dataframe is empty! No file will be written!'

        # Test init
        df_empty = pd.DataFrame()
        key = 'key.csv'
        file_format = 'csv'

        # Method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_empty,
                                                        key, file_format)
            # Log test after method execution
            self.assertIn(log_exp, logm.output[0])

        # Tests after method execution
        self.assertEqual(return_exp, result)


    def test_write_df_to_s3_csv(self):
        """
        Tests the write_df_to_s3() method with a DataFrame and .csv file format
        """
        # Expected Results
        key_exp = 'test.csv'
        return_exp = True
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']],
                              columns=['col1', 'col2'])
        log_exp = f'Writing file to {self.s3_endpoint_url}/' \
                  f'{self.s3_bucket_name}/{key_exp}'

        # Test init
        file_format = 'csv'

        # Method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_exp,
                                                        key_exp, file_format)
            # Log test after method execution
            self.assertIn(log_exp, logm.output[0])

        # Tests after method execution
        data = self.s3_bucket.Object(key=key_exp).get().get('Body') \
                                .read().decode('utf-8')
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))

        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )


    def test_write_df_to_s3_parquet(self):
        """
        Tests the write_df_to_s3() method with a DataFrame and .parquet
        file format
        """
        # Expected Results
        key_exp = 'test.parquet'
        return_exp = True
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']],
                              columns=['col1', 'col2'])
        log_exp = f'Writing file to {self.s3_endpoint_url}/' \
                  f'{self.s3_bucket_name}/{key_exp}'

        # Test init
        file_format = 'parquet'

        # Method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_exp,
                                                        key_exp, file_format)
            # Log test after method execution
            self.assertIn(log_exp, logm.output[0])

        # Tests after method execution
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))

        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )


    def test_write_df_to_s3_wrong_format(self):
        """
        Tests the write_df_to_s3() method with a wrong file format parameter
        """
        # Expected Results
        df_exp =pd.DataFrame([['A', 'B'], ['C', 'D']],
                             columns=['col1', 'col2'])
        key_exp = 'test.parquet'
        format_exp = 'wrong_format'
        log_exp = f'The file format {format_exp} is not supported to be written to S3!'
        exception_exp = WrongFormatException

        # Method execution
        with self.assertLogs() as logm:
            with self.assertRaises(exception_exp):
                self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, format_exp)
            # Log test after method execution
            self.assertIn(log_exp, logm.output[0])


if __name__ == '__main__':
    unittest.main()
