"""
    File: meta_process.py
  Author: Ian Featherston
    Date: 2025-03-13
    Desc: Contains methods for processing meta files.

  Course: Udemy - "Writing Production-Ready ETL Pipelines in Python/Pandas" 
                - by Jan Schwarzlose
                - Section 5 & 6
"""
import collections
from datetime import datetime, timedelta

import pandas as pd

from xetra.common.s3 import S3BucketConnector
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException

class MetaProcess:
    """
    Class for working with the meta file.
    """

    @staticmethod   # No need to include 'self' as a parameter
    def update_meta_file(extract_date_list: list, meta_key: str,
                         s3_bucket_meta: S3BucketConnector):
        """
        Updates the meta file with the latest extract dates.

        Parameters:
            extract_date_list (list): List of extract dates
            meta_key (str): Key to the meta file
            s3_bucket_meta (S3BucketConnector): S3BucketConnector object
        """
        # Create an empty DataFrame using the meta file column names
        df_new = pd.DataFrame(columns=[
            MetaProcessFormat.META_SOURCE_DATE_COL.value,
            MetaProcessFormat.META_PROCESS_COL.value])

        # Filling the date column with extract_date_list
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list

        # Filling the processed column
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = datetime.today() \
                .strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            # If meta file exists -> union DataFrame of old and new meta data is created
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException

            df_all = pd.concat([df_old, df_new])
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # No meta file exists -> only teh new data is used
            df_all = df_new

        # Writing to S3
        s3_bucket_meta.write_df_to_s3(df_all, meta_key,
                                      MetaProcessFormat.META_FILE_FORMAT.value)
        return True


    @staticmethod
    def return_date_list(first_date: str, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Creating a list of dates based on the input first_date and the 
        already processsed dates in the meta file.

        Parameters:
            first_date (str): First date of the extract
            meta_key (str): Key to the meta file
            s3_bucket_meta (S3BucketConnector): S3BucketConnector object

        Returns:
            min_date (str): First date of the extract
            return_date_list (list): List of all dates from min_date to today
        """
        start = datetime.strptime(first_date,
                                  MetaProcessFormat.META_DATE_FORMAT.value) \
                                    .date() - timedelta(days=1)
        today = datetime.today().date()
        s3_client = s3_bucket_meta.session.client('s3')

        try:
            # If meta file exists, create return_date_list using its content
            # Reading meta file
            df_meta = s3_bucket_meta.read_csv_to_df(meta_key)

            # Creating a list of dates from first_date until today
            dates = [start + timedelta(days = x)
                     for x in range(0, (today - start).days + 1)]

            # Creating a set of all dates in meta file
            src_dates = set(pd.to_datetime(
                df_meta[MetaProcessFormat.META_SOURCE_DATE_COL.value]
                ).dt.date)
            dates_missing = set(dates[1:]) - src_dates

            if dates_missing:
                # Determining the earliest date that should be extracted
                min_date = min(set(dates[1:]) - src_dates) - timedelta(days = 1)
                # Creating a list of dates from min_date until today
                return_min_date = (min_date + timedelta(days = 1)) \
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                return_dates = [
                    date.strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
                        for date in dates if date >= min_date
                    ]
            else:
                # Setting values for the earliest date and the list of dates
                return_dates = []
                return_min_date = datetime(2200, 1, 1).date()\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        except s3_client.exceptions.NoSuchKey:
            # No meta file found -> Create date list from (first_date - 1) to today
            return_min_date = first_date
            return_dates = [
                (start + timedelta(days = x)) \
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
                    for x in range(0, (today - start).days + 1)
            ]

        # Returning the earliest date and the list of dates to be processed
        return return_min_date, return_dates
