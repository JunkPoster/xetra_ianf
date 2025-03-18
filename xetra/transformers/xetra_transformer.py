"""
    File: xetra_transformer.py
  Author: Ian Featherston
    Date: 2025-03-13
    Desc: Contains the XetraTransformer class which is used to transform 
            Xetra data.

  Course: Udemy - "Writing Production-Ready ETL Pipelines in Python/Pandas" 
                - by Jan Schwarzlose
                - Section 5 & 6
"""
import logging
from datetime import datetime

# NamedTuple is a class that allows you to create a tuple with named fields
#  - This is a useful way to create a simple data structure like a class,
#       but without the overhead of creating a full class
#  * Much like a 'struct' in C++
from typing import NamedTuple
import pandas as pd

# Import our S3BucketConnector class
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess


class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data.

    src_first_extract_date: Determines the date for extracting the source
    src_columns []: Source column names
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
    Class for Target configuration data.

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
    tgt_col_op_price: str
    tgt_col_clos_price: str
    tgt_col_min_price: str
    tgt_col_max_price: str
    tgt_col_dail_trad_vol: str
    tgt_col_ch_prev_clos: str
    tgt_key: str
    tgt_key_date_format: str
    tgt_format: str


class XetraETL:
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
        self.extract_date, self.extract_date_list = \
            MetaProcess.return_date_list(src_args.src_first_extract_date,
                                         self.meta_key, self.s3_bucket_tgt)
        self.meta_update_list = [date for date in self.extract_date_list \
                                 if date >= self.extract_date]


    def extract(self):
        """
        Reads the source data and concatenates it into on Pandas DataFrame

        Returns:
            data_frame (df): Pandas DataFrame containing the source data
        """
        self._logger.info('Extracting Xetra source files started...')

        # Get the list of files in the source bucket
        files = [
            key for date in self.extract_date_list \
                for key in self.s3_bucket_src.list_files_in_prefix(date)
        ]

        # If there are no files to be extracted -> return an empty DataFrame
        if not files:
            data_frame = pd.DataFrame()
        else:
            data_frame = pd.concat([self.s3_bucket_src.read_csv_to_df(file) \
                                    for file in files], ignore_index=True)

        self._logger.info('Extracting Xetra source files finished.')
        return data_frame


    def transform_report1(self, data_frame: pd.DataFrame):
        """
        Applies the necessary transformation to create report 1

        Parameters:
            data_frame (df): Pandas DataFrame as Input

        Returns:
            data_frame (df): Transformed Pandas DataFrame as Output
        """
        # If the DataFrame is empty -> log and return it as-is
        if data_frame.empty:
            self._logger.info('The dataframe is empty. No transformations will be applied.')
            return data_frame

        self._logger.info('Applying transformations to Xetra source data for report 1 started...')

        # Filtering necessary source columns
        data_frame = data_frame.loc[:, self.src_args.src_columns]

        # Removing rows with missing values
        data_frame.dropna(inplace=True)

        # Calculating opening price per ISIN and day
        data_frame[self.tgt_args.tgt_col_op_price] = data_frame \
            .sort_values(by=[self.src_args.src_col_time]) \
                .groupby([
                    self.src_args.src_col_isin,
                    self.src_args.src_col_date
                    ])[self.src_args.src_col_start_price] \
                        .transform('first')

        # Calculating the closing price per ISIN and day
        data_frame[self.tgt_args.tgt_col_clos_price] = data_frame \
            .sort_values(by=[self.src_args.src_col_time]) \
                .groupby([
                    self.src_args.src_col_isin,
                    self.src_args.src_col_date
                    ])[self.src_args.src_col_start_price] \
                        .transform('last')

        # Renaming columns per target configuration
        data_frame.rename(columns={
            self.src_args.src_col_min_price: self.tgt_args.tgt_col_min_price,
            self.src_args.src_col_max_price: self.tgt_args.tgt_col_max_price,
            self.src_args.src_col_traded_vol: self.tgt_args.tgt_col_dail_trad_vol
            }, inplace=True)

        # Aggregating per ISIN and day -> opening price, closing price,
        # minimum price, maximum price, & daily traded volume
        data_frame = data_frame.groupby([
            self.src_args.src_col_isin,
            self.src_args.src_col_date], as_index=False)\
                .agg({
                    self.tgt_args.tgt_col_op_price: 'min',
                    self.tgt_args.tgt_col_clos_price: 'min',
                    self.tgt_args.tgt_col_min_price: 'min',
                    self.tgt_args.tgt_col_max_price: 'max',
                    self.tgt_args.tgt_col_dail_trad_vol: 'sum'})

        # Change of the current day's closing price compared to the
        # previous day's closing price (in %)
        data_frame[self.tgt_args.tgt_col_ch_prev_clos] = data_frame \
            .sort_values(by=[self.src_args.src_col_date]) \
                .groupby([self.src_args.src_col_isin]) \
                    [self.tgt_args.tgt_col_op_price] \
                        .shift(1)
        data_frame[self.tgt_args.tgt_col_ch_prev_clos] = (
            data_frame[self.tgt_args.tgt_col_op_price] \
            - data_frame[self.tgt_args.tgt_col_ch_prev_clos]) \
            / data_frame[self.tgt_args.tgt_col_ch_prev_clos ] \
            * 100   # Change into %

        # Round it to 2 decimal places
        data_frame = data_frame.round(decimals=2)

        # Removing the day before extract_date
        data_frame = data_frame[data_frame.Date >= self.extract_date] \
            .reset_index(drop=True)

        self._logger.info('Applying transformations to Xetra source data finished...')
        return data_frame


    def load(self, data_frame: pd.DataFrame):
        """
        Saves a Pandas DataFrame to the target

        Parameters:
            data_frame (df): Pandas DataFrame as Input
        """
        # Creating target key
        target_key = (
            f'{self.tgt_args.tgt_key}'
            f'{datetime.today().strftime(self.tgt_args.tgt_key_date_format)}.'
            f'{self.tgt_args.tgt_format}'
        )

        # Writing to target
        self.s3_bucket_tgt.write_df_to_s3(data_frame, target_key,
                                          self.tgt_args.tgt_format)
        self._logger.info('Xetra target data successfully written.')

        # Updating meta file
        MetaProcess.update_meta_file(self.meta_update_list,
                                     self.meta_key, self.s3_bucket_tgt)
        self._logger.info('Xetra meta file successfully updated.')
        return True


    def etl_report1(self):
        """
        ETL
        Extract, Transform and Load the data to create report 1
        """
        # Extraction
        data_frame = self.extract()

        # Transformation
        data_frame = self.transform_report1(data_frame)

        # Load
        self.load(data_frame)
        return True
