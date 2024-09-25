import logging
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from abc import ABC, abstractmethod
from typing import Union


class DataStrategy(ABC):
    """ Abstract class for data cleaning strategy """
    @abstractmethod
    def handle_data(self, data: pd.DataFrame) -> pd.DataFrame:
        pass

class PreprocessingStrategy(DataStrategy):
    """ Strategn for Data preprocessing """

    def handle_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """" Preprocess data """

        try:
            data = data.drop(columns=[
                'order_approved_at', 
                'order_delivered_carrier_date',
                'order_delivered_customer_date',
                'order_purchase_timestamp',
            ])
            data['product_weight_g'] = data['product_weight_g'].fillna(data['product_weight_g'].median(), inplace=True)
            data['product_length_cm'] = data['product_length_cm'].fillna(data['product_length_cm'].median(), inplace=True)
            data['product_height_cm'] = data['product_height_cm'].fillna(data['product_height_cm'].median(), inplace=True)
            data['product_width_cm'] = data['product_width_cm'].fillna(data['product_width_cm'].median(), inplace=True)
            data['review_comment_message'] = data['review_comment_message'].fillna('no review', inplace=True)

            data = data.select_dtypes(include=[np.number])
            cols_to_drop = ['customer_zip_code_prefix', 'order_item_id']
            data = data.drop(columns=cols_to_drop)
            return data
        except Exception as e:
            logging.error('Error in preprocessing the data in <src/data_cleaning>')
            raise e 
        
class SplitDataStrategy(DataStrategy):
    """ Strategy for spliting data into training and testing """

    def handle_data(self, data: pd.DataFrame) -> Union[pd.DataFrame, pd.Series]:
        """" Split data into training and testing 
        args:
            data: pd.DataFrame: data to split
        return:
            pd.DataFrame: training data
            pd.Series: testing data
        """
        try:
            x = data.drop(columns=['review_score'])
            y = data['review_score']

            x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2)
            return x_train, x_test, y_train, y_test
        except Exception as e:
            logging.error('Error in spliting the data in <src/data_cleaning>')
            raise e
        


class DataCleaning:
    """  Data cleaning class """

    def __init_(self, data: pd.DataFrame, strategy: DataStrategy):
        """"
        Initialize the class with the data and strategy
        args:
            data: pd.DataFrame: data to clean
            strategy: DataStrategy: strategy to clean the data
        return:
            None
        """
        self.data = data
        self.strategy = strategy
    
    def handle_data(self) -> Union[pd.DataFrame, pd.Series]:
        """"
        Handle data
        """
        try:
            return self.strategy.handle_data(self.data)
        except Exception as e:
            logging.error('Error in handling the data in <src/data_cleaning>')
            raise e