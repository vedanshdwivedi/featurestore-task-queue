from run import celery_app

from jobs.xgboostregressor.xgbregressor import XGBRegressorPipeline
from jobs.Predictions import updateTaskMetadataPostgres, fetchTaskById, createTaskLogs


@celery_app.task()
def xgbRegressorTraining(projectId: int, taskId: int):
    try:
        updateTaskMetadataPostgres(taskId, newStatus="RUNNING")
        createTaskLogs(taskId, projectId, "Starting XGBoost Regressor Modelling Task")
        pipeline = XGBRegressorPipeline(projectId=projectId)
        pipeline.trainModel()
        updateTaskMetadataPostgres(taskId, newStatus="SUCCESS")
        createTaskLogs(taskId, projectId, "XGBoost Regressor Modelling Task Completed without Errors")
    except Exception as ex:
        updateTaskMetadataPostgres(taskId, newStatus="ERROR")
        createTaskLogs(taskId, projectId, f"XGBoost Regressor Modelling Task Failed with Errors : {ex}")
    finally:
        return True


def xgbRegressorPrediction(projectId: int):
    pass
