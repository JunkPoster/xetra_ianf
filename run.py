"""
    File: run.py
  Author: Ian Featherston
    Date: 2025-03-13
    Desc: Contains the main script to run the ETL pipeline.

  Course: Udemy - "Writing Production-Ready ETL Pipelines in Python/Pandas" 
                - by Jan Schwarzlose
                - Section 5
'"""''

import logging
import logging.config

import yaml


def main():
    """
    Entry point to run the Xetra ETL Job
    """
    # Parsing YAML File
    config_path = 'C:/Users/TheNo/OneDrive/Python Projects/xetra_project/xetra_ianf/configs/xetra_report1_config.yaml'
    config = yaml.safe_load(open(config_path))
    
    # Configure Logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)   # Configure logging as a dictionary
    logger = logging.getLogger(__name__)    # Initialize the logger
    logger.info("This is a test.")


# Run the main function
if __name__=='__main__':
    main()
