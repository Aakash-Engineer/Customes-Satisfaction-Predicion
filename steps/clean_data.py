import os
import logging
import pandas as pd
from zenml import step

@step
def clean_data(data: pd.DataFrame) -> None:
    pass