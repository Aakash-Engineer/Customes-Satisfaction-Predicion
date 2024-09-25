import pandas as pd
from steps.clean_data import clean_data
from steps.model_train import train_model
from steps.ingest_data import ingest_data
from steps.evaluate_model import evaluate_model
from zenml import pipeline



@pipeline(name='training_pipeline', enable_cache=True)
def train_pipeline(data_path: str) -> None:
    """"
    Training Pipeine
    args:
        data_path: str: path to the csv file
    return:
        None
    """
    df = ingest_data(path=data_path)
    clean_data(df)
    train_model(df)
    evaluate_model(df)
