from datetime import datetime
from typing import Union, Dict, List, Tuple

from emailClient import EmailConnection
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


def run_prediction_pipeline(
    project_id: int,
    prediction_filename: Union[str, None],
    model_filename: Union[str, None],
) -> Union[None, pd.DataFrame]:
    if prediction_filename is None or model_filename is None:
        return None
    project_folder = os.path.join(
        os.environ.get("LOCAL_FILE_PATH", "files"), f"project-{project_id}"
    )
    prediction_dataset_path = os.path.join(project_folder, prediction_filename)
    model_path = os.path.join(project_folder, model_filename)
    try:
        transformer = get_initialised_tranformation_object(
            project_id=project_id, dataset_path=prediction_dataset_path
        )
        df = transformer.impute(model_path, prediction_dataset_path)
        return df
    except Exception as ex:
        raise Exception(
            f"[JOBS][transform][run_prediction_pipeline] Unable to get transformation object : {ex} "
        )


def download_project_files_locally(
    project_id: int,
) -> Tuple[Union[str, None], Union[str, None]]:
    prediction_filename = None
    model_filename = None
    file_list = fetch_project_files_by_project_id(project_id)
    if len(file_list) > 0:
        transformation_file = None
        prediction_file = None
        model_file = None
        for file in file_list:
            # developers
            if file.get("file_category", "") == "TRANSFORMATION":
                transformation_file = file
            # user
            if file.get("file_category") == "PREDICTION_DATASET":
                prediction_file = file
                prediction_filename = prediction_file.get("filename")
            # developers
            if file.get("file_category") == "MODEL":
                model_file = file
                model_filename = model_file.get("filename")
        if prediction_file is not None:
            for file in [prediction_file, transformation_file, model_file]:
                download_blob_locally(
                    project_id=project_id,
                    file_name=file.get("filename"),
                    container_name=file.get("container"),
                )
    return prediction_filename, model_filename


def fetch_project_files_by_type(project_id: int, file_type: str) -> pd.DataFrame:
    query = f"""SELECT * FROM dt.files WHERE pid = {project_id} AND file_category = '{file_type}' 
                AND deleted = false"""
    try:
        df = pd.read_sql(query, engine)
    except Exception as ex:
        raise Exception(ex)
    return df


def soft_delete_files(file_id: List[int]):
    file_id = [int(x) for x in file_id]
    file_id_list = str(file_id).strip("[]")
    query = f"""UPDATE dt.files SET deleted = true WHERE fid in ({file_id_list})"""
    try:
        engine.execute(query)
    except Exception as ex:
        raise Exception(ex)


def update_task_request_metadata(request_id: int, status: str) -> None:
    query = (
        f"""UPDATE dt.task_queue SET status='{status}' WHERE task_id = {request_id}"""
    )
    try:
        engine.execute(query)
    except Exception as ex:
        raise Exception(
            f"[JOBS][transform][update_task_request_metadata] Unable to update request metadata : {ex} "
        )


def create_file_metadata_postgres(
    project_id: int, file_name: str, file_type: str, url: str
) -> None:
    existing_files = fetch_project_files_by_type(project_id, file_type)
    if len(existing_files) > 0:
        file_id_list = existing_files["fid"].unique().tolist()
        soft_delete_files(file_id_list)
    container = hash_string_using_secret_key(f"project-{project_id}")
    query = f"""insert into dt.files (pid, filename, file_category, container, download_link) 
    values ({project_id}, '{file_name}', '{file_type}', '{container}', '{url}')"""
    try:
        engine.execute(query)
    except Exception as ex:
        raise Exception(
            f"[JOBS][transform][create_file_metadata_postgres] Unable to create file metadata : {ex} "
        )


def send_notification_email(
    project_id: int, success: bool = True, exception: str = None
) -> None:
    project = fetch_project_by_id(project_id)
    project_email = project["project"].get("notification")
    email_client = EmailConnection.getInstance()
    if success:
        subject = "Predictions have been uploaded"
        body = f"""You requested for predictions for the project : {project['project']['project_name']}. 
                    This is to notify you that your predictions have been uploaded for the project and 
                    can be downloaded from the projects page.
                """
    else:
        subject = "Predictions Failed"
        body = f"""You requested for predictions for the project : {project['project']['project_name']}. 
                    This is to notify you that your predictions can't be generated. We request you to 
                    put a remark on the project and stating the reason [{exception}] to your developer.
                    We regret the inconvenience caused. 
                """
    email_client.send_email(receipient=project_email, subject=subject, content=body)


@celery_app.task()
def run_prediction_jobs(project_id: int, request_id: int):
    try:
        create_project_dir(project_id)
        prediction_filename, model_filename = download_project_files_locally(project_id)
        predicted_dataset = run_prediction_pipeline(
            project_id, prediction_filename, model_filename
        )
        save_dataset_locally(
            df=predicted_dataset, project_id=project_id, filename="predictions.xlsx"
        )
        dataset_path = os.path.join(
            os.path.join(
                os.environ.get("LOCAL_FILE_PATH", "files"), f"project-{project_id}"
            ),
            "predictions.xlsx",
        )
        try:
            url = upload_dataset_to_blob(
                dataset_path=dataset_path, project_id=project_id
            )
        except Exception as ex:
            raise Exception(
                f"[JOBS][transform][run_prediction_jobs] Unable to upload dataset to blob : {ex} "
            )
        try:
            create_file_metadata_postgres(
                project_id=project_id,
                file_name="predictions.xlsx",
                file_type="PREDICTION",
                url=url,
            )
        except Exception as ex:
            raise Exception(
                f"[JOBS][transform][run_prediction_jobs] Unable to upload file metadata : {ex} "
            )
        send_notification_email(project_id)

    except Exception as ex:
        send_notification_email(project_id, success=False, exception=f"{ex}")
        raise Exception(ex)
    finally:
        del_project_dir(project_id)
        update_task_request_metadata(request_id, "COMPLETED")
