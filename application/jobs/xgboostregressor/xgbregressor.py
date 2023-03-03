import os
from typing import List

import joblib

from utilities.utils import *
from jobs.Projects import getProjectData, getProjectSettings, getProjectFiles, updateProjectSettings
from jobs.Files import create_file_entry_in_postgres, handle_existing_file_metadata_delete_by_category
from skforecast.ForecasterAutoreg import ForecasterAutoreg
from jobs.TransformationBase import TransformationBase
from skforecast.model_selection import grid_search_forecaster
from xgboost import XGBRegressor

mongoClient = get_mongo_client()
engine = get_db_engine()
blobClient = get_blob_client()


class XGBRegressorPipeline:
    def __init__(self, projectId: int):
        self.projectId = projectId
        self.project_data = getProjectData(projectId).get("project")
        self.project_settings = getProjectSettings(projectId)
        self.project_files = getProjectFiles(projectId).get("files")
        self.model = ForecasterAutoreg(regressor=XGBRegressor(random_state=123), lags=24)
        self.transformer = False
        self.trainingDatasetPath = None
        if len(self.project_settings.get("paramsGrid", [])) < 1:
            # Use Default Params Grid
            self.paramsGrid = {
                "learning_rate": [0.02, 0.03],
                "max_depth": [5, 10],
                "n_estimators": [100, 500]
            }
        else:
            # Inherit Grid From Settings
            self.paramsGrid = self.project_settings["paramsGrid"]

    def getProjectData(self):
        return self.project_data

    def getProjectSettings(self):
        return self.project_settings

    def getProjectFiles(self):
        return self.project_files

    def clearProjectFiles(self):
        del_project_dir(self.projectId)

    def load_transformation_class(self):
        try:
            download_blob_locally(self.projectId, "transformation.py", self.project_data["container"])
            transformFilePath = os.path.join(os.environ.get("LOCAL_FILE_PATH", "files"), f"project-{self.projectId}",
                                             "transformation.py")
            obj = SourceFileLoader("transformation", transformFilePath).load_module().Transformation(
                self.project_data["title"])
            self.transformer = TransformationBase(obj)
        except Exception as ex:
            raise Exception(ex)

    def getParamsGrid(self):
        pass

    @staticmethod
    def splitDataset(df: pd.DataFrame, features: List[str], label: str):
        trainDataset = df[: int(0.7 * len(df))]
        valDataset = df[int(0.7 * len(df)): int(0.85 * len(df))]
        testDataset = df[int(0.85 * len(df)):]
        return {
            "train": {"X": trainDataset[features], "Y": trainDataset[[label]]},
            "test": {"X": testDataset[features], "Y": testDataset[[label]]},
            "validation": {"X": valDataset[features], "Y": valDataset[[label]]},
        }

    def downloadTrainingDataset(self):
        try:
            for file in self.project_files:
                if file["category"] == "DATASET":
                    download_blob_locally(self.projectId, file["filename"], self.project_data["container"])
                    self.trainingDatasetPath = os.path.join(os.environ.get("LOCAL_FILE_PATH", "files"),
                                                            f"project-{self.projectId}",
                                                            file["filename"])
        except Exception as ex:
            raise Exception(ex)

    def preTrainingChecks(self):
        self.load_transformation_class()
        self.downloadTrainingDataset()
        if self.trainingDatasetPath is None:
            raise Exception("Dataset not found")

    def trainModel(self):
        try:
            self.preTrainingChecks()
            df = self.transformer.transformDataset(self.trainingDatasetPath)
            if len(self.project_settings.get("features", [])) == 0:
                raise Exception("Features Not Defined in Settings")
            features = self.project_settings["features"]
            if self.project_settings.get("label") is None:
                raise Exception("Label is not defined")
            label = self.project_settings["label"]
            splitDataset = self.splitDataset(df, features, label)
            _ = grid_search_forecaster(
                forecaster=self.model,
                y=df[: int(0.85 * len(df))][label],
                exog=df[: int(0.85 * len(df))][features],
                param_grid=self.paramsGrid,
                lags_grid=[24, 48, 72, [1, 2, 3, 23, 24, 25, 71, 72, 73]],
                steps=36,
                refit=False,
                metric="mean_squared_error",
                initial_train_size=len(splitDataset["train"]["X"]),
                fixed_train_size=False,
                return_best=True,
                verbose=False,
            )
            print("[XGBRegressor Pipeline] Model Trained Successfully, Saving Model Locally")
            joblib.dump(self.model, os.path.join(os.environ.get("LOCAL_FILE_PATH", "files"), f"project-"
                                                                                             f"{self.projectId}",
                                                 "model.pkl"))
            downloadLink = upload_dataset_to_blob(os.path.join(os.environ.get("LOCAL_FILE_PATH", "files"), f"project-"
                                                                                                           f"{self.projectId}",
                                                               "model.pkl"), self.project_data["container"])
            handle_existing_file_metadata_delete_by_category(self.projectId, "MODEL")
            create_file_entry_in_postgres(self.projectId, "model.pkl", self.project_data["container"], downloadLink,
                                          "MODEL")
            self.project_settings["ready"] = True
            updateProjectSettings(self.projectId, self.project_settings)
        except Exception as ex:
            raise Exception(ex)
        finally:
            self.clearProjectFiles()

    def fetchPredictions(self):
        pass
