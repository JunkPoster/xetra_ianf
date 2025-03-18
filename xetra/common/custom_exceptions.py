"""
    File: custom_exceptions.py
  Author: Ian Featherston
    Date: 2025-03-13
    Desc: Contains custom exceptions for the ETL pipeline.

  Course: Udemy - "Writing Production-Ready ETL Pipelines in Python/Pandas" 
                - by Jan Schwarzlose
                - Section 5 & 6
"""

class WrongFormatException(Exception):
    """
    WrongFormatException Class
    
    Exception raised when the format type given as a parameter is not supported.
    """


class WrongMetaFileException(Exception):
    """
    WrongMetaFileException Class
    
    Exception raised when the meta file is not in the correct format.
    """
