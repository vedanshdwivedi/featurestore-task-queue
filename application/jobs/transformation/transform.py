from typing import Union, Dict, List
import pandas as pd

from run import celery_app
from utilities.utils import *

engine = get_db_engine()
blob = get_blob_client()


def fetch_project_by_id(
    project_id: int,
) -> Dict[str, Union[str, int, Dict[str, Union[int, str]]]]:
    response_dict = {}
    query = f"SELECT * FROM dt.projects WHERE pid = {project_id}"
    df = pd.read_sql(query, engine)
    if len(df) > 0:
        response_dict["project"] = df.to_dict(orient="records")[0]
    return response_dict


def fetch_project_files_by_project_id(
    project_id: int,
) -> List[Dict[str, Union[int, str, bool]]]:
    response_dict = {"files": []}
    query = f"SELECT * FROM dt.files WHERE pid = {project_id} AND deleted = false"
    df = pd.read_sql(query, engine)
    if len(df) > 0:
        response_dict["files"] = df.to_dict(orient="records")
    return response_dict.get("files", [])


@celery_app.task()
def create_predictions(project_id: int, file_id: int):
    try:
        create_project_dir(project_id)
        file_list = fetch_project_files_by_project_id(project_id)
        if len(file_list) > 0:
            transformation_file = None
            for file in file_list:
                if file.get("file_category", "") == "TRANSFORMATION":
                    transformation_file = file
                    break
            get_transformation_file(
                project_id=project_id,
                file_name=transformation_file.get("filename"),
                container_name=transformation_file.get("container"),
            )
    except Exception as ex:
        raise Exception(ex)
    finally:
        print("Completed Task")
        del_project_dir(project_id)
