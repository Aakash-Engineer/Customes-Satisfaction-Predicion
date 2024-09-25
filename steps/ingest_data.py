import logging
import os
import pandas as pd
from zenml import step


class IngestData:
    """Ingest data from csv file"""
    def __init__(self, path: str):
        """
        Initialize the class with the path to the csv file
        args:
            path: str: path to the csv file
        return:
            None
        """
        self.path = path
    
    def get_data(self) -> pd.DataFrame:

        """
        Read the csv file and return the data
        args:
            None
        return:
            pd.DataFrame: data read from the csv file
        """
        return pd.read_csv(self.path)
    

@step
def ingest_data(path: str):
    """
    Ingest data from csv file
    args:
        ptah: str: path to the csv file
    return:
        pd.DataFrame: data read from the csv file
    """

    try:
        ingest_data = IngestData(path)
        data = ingest_data.get_data()
        return data
    except Exception as e:
        logging.error('Error in reading the data from the csv file')
        raise e