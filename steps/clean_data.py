import os
import logging
import pandas as pd
from zenml import step
from src.data_cleaning import PreprocessingStrategy, SplitDataStrategy, DataCleaning
from typing_extensions import Annotated
from typing import Tuple

@step
def clean_data(data: pd.DataFrame) -> Tuple[
    Annotated[pd.DataFrame, 'x_train'],
    Annotated[pd.DataFrame, 'x_test'],
    Annotated[pd.Series, 'y_train'],
    Annotated[pd.Series, 'y_test']
]:
    try:
        process_strategy = PreprocessingStrategy()
        data_cleaning = DataCleaning(data, process_strategy)
        processeddata = data_cleaning.handle_data()

        split_strategy = SplitDataStrategy()
        split_data = DataCleaning(processeddata, split_strategy)
        x_train, x_test, y_train, y_test = split_data.handle_data()
        logging.info('Data cleaning completed')
    except Exception as e:
        logging.error('Error in cleaning the data in <steps/clean_data>')
        raise e