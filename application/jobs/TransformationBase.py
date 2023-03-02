from typing import Optional

import pandas as pd


class TransformationBase:
    def __init__(self, TransformClass):
        self.transform = TransformClass

    def loadDataset(self, datasetPath: str) -> pd.DataFrame:
        # loadDataset() is a required member function in Transformation Class
        try:
            df = self.transform.loadDatset(datasetPath)
        except Exception as ex:
            raise Exception(f"Error in loading Dataset : {ex}")
        return df

    def transformDataset(self, datasetPath: str) -> pd.DataFrame:
        # transform() is a required member function in Transformation Class
        try:
            df = self.transform.transform(datasetPath)
        except Exception as ex:
            raise Exception(f"Error in Transforming Datatset : {ex}")
        return df

    def impute(self, modelPath: str = None, predictionDatasetPath: str = None) -> Optional[pd.DataFrame]:
        # impute(modelPath, predictionDfPath) is a required member function in Transformation Class
        try:
            df = self.transform.impute(modelPath, predictionDatasetPath)
        except Exception as ex:
            raise Exception(f"Error in fetching predictions : {ex}")
        return df
