from typing import Optional
from datetime import datetime

from utilities.utils import get_mongo_client, get_db_engine, get_blob_client
import pandas as pd

mongoClient = get_mongo_client()
engine = get_db_engine()
blobClient = get_blob_client()


def fetchTaskById(taskId: int):
    query = f"""SELECT * FROM predictions WHERE "taskId" = {taskId}"""
    try:
        df = pd.read_sql(query, engine)
        return df.to_dict(orient="records")[0]
    except Exception as ex:
        raise Exception(ex)


def updateTaskMetadataPostgres(taskId: int, newStatus: Optional[str] = None, graphId: Optional[str] = None, ):
    if newStatus is not None or graphId is not None:
        currentTime = datetime.now().replace(microsecond=0)
        query = "UPDATE predictions SET "
        if newStatus is not None:
            query += f""" "status" = '{newStatus}' """
        if graphId is not None:
            if newStatus is None:
                query += f""" "graphId" = {graphId} """
            else:
                query += f""" , "graphId" = {graphId} """
        query += f""" , "updatedAt" = '{currentTime}' WHERE "taskId" = {taskId}"""
        try:
            engine.execute(query)
        except Exception as ex:
            raise Exception(f"Error in updating Task Metadata : {ex}")


def createTaskLogs(taskId: int, projectId: int, action: str, comment: str = "-"):
    try:
        mongoClient.db.predictionLogs.insert_one({"taskId": taskId, "projectId": projectId, "action": action,
                                                  "comment": comment, "createdAt": datetime.now().replace(
                microsecond=0)})
    except Exception as ex:
        raise Exception(ex)
