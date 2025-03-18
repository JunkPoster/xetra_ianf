"""
    File: run.py
  Author: Ian Featherston
    Date: 2025-03-13
    Desc: Contains entry point for the ETL job.

  Course: Udemy - "Writing Production-Ready ETL Pipelines in Python/Pandas" 
                - by Jan Schwarzlose
                - Section 5 & 6

My Notes: If I were to refactor this code, I would consider parameterizing
        the various status/error messages for more flexibility and easier
        maintenance. I would also consider adding more error handling and
        logging to the code, and likely a profiler to monitor performance.

        I think there's a lot that could be modularized as well, such as the
        S3BucketConnector class, which could be made more generic and used
        for other projects. I would also add more unit tests.
"""
import argparse         # For parsing command line arguments
import logging
import logging.config

import yaml             # For parsing our YAML configuration file

from xetra.common.s3 import S3BucketConnector
from xetra.transformers.xetra_transformer import XetraETL, XetraSourceConfig, XetraTargetConfig


def main():
    """
    Entry point to run the Xetra ETL Job
    """
    # Parsing the YAML Configuration File
    parser = argparse.ArgumentParser(description='Run the Xetra ETL job.')
    parser.add_argument('config', help='A configuration file in YAML format.')
    args = parser.parse_args()

    # Safely open the configuration file
    with open(args.config, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    # Configure Logging
    log_config = config['logging']          # Read the logging configuration
    logging.config.dictConfig(log_config)   # Configure logging as a dictionary
    logger = logging.getLogger(__name__)    # Initialize the logger

    # Reading S3 Configuration
    s3_config = config['s3']

    # Creating the S3BucketConnector instances based on the configurations
    s3_bucket_src = S3BucketConnector(
        access_key=s3_config['access_key'],
        secret_key=s3_config['secret_key'],
        endpoint_url=s3_config['src_endpoint_url'],
        bucket=s3_config['src_bucket'],
        region_name=s3_config['src_region']
    )
    s3_bucket_trg = S3BucketConnector(
        access_key=s3_config['access_key'],
        secret_key=s3_config['secret_key'],
        endpoint_url=s3_config['tgt_endpoint_url'],
        bucket=s3_config['tgt_bucket'],
        region_name=s3_config['tgt_region']
    )

    # Reading configurations for the respective buckets
    source_config = XetraSourceConfig(**config['source'])       # Source
    target_config = XetraTargetConfig(**config['target'])       # Target
    meta_config = config['meta']                                # Meta Config

    # Create a XetraETL class instance
    logger.info('Xetra ETL Job Started')
    xetra_etl = XetraETL(
        s3_bucket_src,
        s3_bucket_trg,
        meta_config['meta_key'],
        source_config,
        target_config
    )

    # Running ETL Job for Xetra Report 1
    xetra_etl.etl_report1()
    logger.info('Xetra ETL Job Completed')


# Run the main function
# - This conditional statement is used to prevent the code from running if 
#       the module is imported
if __name__=='__main__':
    main()
