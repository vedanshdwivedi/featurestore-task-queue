from typing import Union, Dict
import pandas as pd

from run import db


def fetch_project_by_id(
    project_id: int,
) -> Dict[str, Union[str, int, Dict[str, Union[int, str]]]]:
    response_dict = {}
    query = f"SELECT * FROM dt.projects WHERE project_id = {project_id}"
    df = pd.read_sql(query, db)
    if len(df) > 0:
        response_dict["project"] = df.to_dict(orient="records")[0]
    return response_dict


def create_predictions(project_id: int, file_id: int) -> None:
    project = fetch_project_by_id(project_id)
    print(project)

