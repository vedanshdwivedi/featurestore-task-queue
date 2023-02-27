from typing import Dict, Union, List
import pandas as pd
from flask import jsonify

from utilities.utils import *

mongoClient = get_mongo_client()
engine = get_db_engine()
blobClient = get_blob_client()

def getProjectData(projectId: int) -> Dict[str, Union[str, int, Dict]]:
    response_dict = {}
    query = f"SELECT * FROM projects where pid={projectId} AND deleted = {False}"
    df = pd.read_sql(query, engine)
    if len(df) > 0:
        response_dict["project"] = df.to_dict(orient="records")[0]
    return response_dict

def getProjectFiles(projectId: int) -> Dict[str, List[Dict[str, int]]]:
    response_dict = {}
    query = f"SELECT * FROM files where pid={projectId} and deleted = {False}"
    df = pd.read_sql(query, engine)
    if len(df) > 0:
        response_dict["files"] = df.to_dict(orient="records")
    return response_dict


def getProjectSettings(projectId: int) -> Dict[str, Union[str, int, Dict]]:
    response_dict = {}
    settings = mongoClient.db.settings.find_one({"projectId": projectId})
    if settings is not None:
        response_dict = settings
        settings["_id"] = str(settings["_id"])
    return response_dict