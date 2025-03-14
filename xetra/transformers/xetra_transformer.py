"""
    File: xetra_transformer.py
  Author: Ian Featherston
    Date: 2025-03-13
    Desc: Contains the XetraTransformer class which is used to transform 
            Xetra data.

  Course: Udemy - "Writing Production-Ready ETL Pipelines in Python/Pandas" 
                - by Jan Schwarzlose
                - Section 5
"""

import logging

# NamedTuple is a class that allows you to create a tuple with named fields
#  - This is a useful way to create a simple data structure like a class,
#       but without the overhead of creating a full class
#  - Much like a 'struct' in C++
from typing import NamedTuple   

# Import our S3BucketConnector class
from xetra.common.s3 import S3BucketConnector 


class XetraSourceConfig(NamedTuple):
    """
    Class for source configuraiton data.

    src_first_extract_date: Determines the date for extracting the source
    src_columns: Source column names
    src_col_date: Column name for Date in source
    src_col_isin: Column name for ISIN in source
    src_col_time: Column name for Time in source
    src_col_start_price: Column name for Starting Price in source
    src_col_min_price: Column name for Minimum Price in source
    src_col_max_price: Column name for Maximum Price in source
    src_col_traded_vol: Column name for Traded Volume in source
    """
    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str

class XetraTargetConfig(NamedTuple):
    """
    Class for Target configuraiton data.

    tgt_col_isin: Column name for ISIN in target
    tgt_col_date: Column name for Date in target
    tgt_op_price: Column name for Opening Price in target
    tgt_col_clos_price: Column name for Closing Price in target
    tgt_col_min_price: Column name for Minimum Price in target
    tgt_col_max_price: Column name for Maximum Price in target
    tgt_col_dail_traded_vol: Column name for Daily Traded Volume in target
    tgt_col_ch_prev_clos: Column name for Change to Previous Close in target
    tgt_key: Basic key for Target file
    tgt_key_date_format: Date format of target file key
    tgt_format: File format of the target file
    """
    tgt_col_isin: str
    tgt_col_date: str
    tgt_op_price: str
    tgt_col_clos_price: str
    tgt_col_min_price: str
    tgt_col_max_price: str
    tgt_col_dail_traded_vol: str
    tgt_col_ch_prev_clos: str
    tgt_key: str
    tgt_key_date_format: str
    tgt_format: str

class XetraETL():
    """
    Reads the Xetra data, transforms it and writes it to the target.
    """

    def __init__(self, s3_bucket_src: S3BucketConnector,
                 s3_bucket_tgt: S3BucketConnector, meta_key: str,
                 src_args: XetraSourceConfig, tgt_args: XetraTargetConfig):
        """
        Constructor for XetraTransformer

        Parameters:
            s3_bucket_src (S3BucketConnector): Connector for source bucket
            s3_bucket_tgt (S3BucketConnector): Connector for target bucket
            meta_key (str): Used as 'self.meta_key' -> key of meta file
            src_args (XetraSourceConfig): NamedTuple class w/ Source config data
            tgt_args (XetraTargetConfig): NamedTuple class w/ Target config data
        """
        self._logger = logging.getLogger(__name__)  # Initialize the logger

        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_tgt = s3_bucket_tgt
        self.meta_key = meta_key
        self.src_args = src_args
        self.tgt_args = tgt_args
        self.extract_date = ''
        self.extract_date_list = ''
        self.meta_update_list = ''


        def extract(self):
            pass


        def transform_report1(self):
            pass


        def load(self):
            pass


        def etl_report1(self):
            pass
