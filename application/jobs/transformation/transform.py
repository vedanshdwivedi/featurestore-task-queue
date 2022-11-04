from typing import Union, Dict
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


@celery_app.task()
def create_predictions(project_id: int, file_id: int):
    try:
        project = fetch_project_by_id(project_id)
        return project
    except Exception as ex:
        # raise Exception(ex)
        return f"Error : {ex}"
