from utilities.utils import *
from jobs.Projects import getProjectData, getProjectSettings, getProjectFiles

mongoClient = get_mongo_client()
engine = get_db_engine()
blobClient = get_blob_client()


class XGBRegressorPipeline:
    def __init__(self, projectId: int):
        self.projectId = projectId
        self.project_data = getProjectData(projectId).get("project")
        self.project_settings = getProjectSettings(projectId)
        self.project_files = getProjectFiles(projectId).get("files")

    def getProjectData(self):
        return self.project_data

    def getProjectSettings(self):
        return self.project_settings

    def getProjectFiles(self):
        return self.project_files

    def load_transformation_class(self):
        download_blob_locally(self.projectId, "dataset.xlsx")

    def getParamsGrid(self):
        pass

    def splitDataset(self):
        pass

    def trainModel(self):
        pass

    def fetchPredictions(self):
        pass