from typing import Union, Dict, List

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
    project_id: int, prediction_filename: Union[str, None]
) -> Union[None, pd.DataFrame]:
    if prediction_filename is None:
        return None
    project_folder = os.path.join(
        os.environ.get("LOCAL_FILE_PATH", "files"), f"project-{project_id}"
    )
    prediction_dataset_path = os.path.join(project_folder, prediction_filename)
    try:
        transformer = get_initialised_tranformation_object(
            project_id=project_id, dataset_path=prediction_dataset_path
        )
        df = transformer.impute()
        return df
    except Exception as ex:
        raise Exception(
            f"[JOBS][transform][run_prediction_pipeline] Unable to get transformation object : {ex} "
        )


def download_project_files_locally(project_id: int) -> Union[str, None]:
    prediction_filename = None
    file_list = fetch_project_files_by_project_id(project_id)
    if len(file_list) > 0:
        transformation_file = None
        prediction_file = None
        for file in file_list:
            if file.get("file_category", "") == "TRANSFORMATION":
                transformation_file = file
            if file.get("file_category") == "PREDICTION_DATASET":
                prediction_file = file
        if prediction_file is not None:
            prediction_filename = prediction_file.get("filename")
            for file in [prediction_file, transformation_file]:
                download_blob_locally(
                    project_id=project_id,
                    file_name=file.get("filename"),
                    container_name=file.get("container"),
                )
    return prediction_filename


def create_file_metadata_postgres(
    project_id: int, file_name: str, file_type: str, url: str
) -> None:
    container = hash_string_using_secret_key(f"project-{project_id}")
    query = """insert into dt.files (pid, filename, file_category, container, metadata, download_link) 
    values (%(project_id)s, %(filename)s, %(filetype)s, %(container)s, %(metadata)s, %(url)s)"""
    try:
        engine.execute(
            query
            % {
                "project_id": project_id,
                "filename": file_name,
                "file_category": file_type,
                "container": container,
                "metadata": {},
                "download_link": url,
            }
        )
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
    email_client.send_email(receipient=project_email, subject=subject, body=body)


@celery_app.task()
def run_prediction_jobs(project_id: int):
    try:
        create_project_dir(project_id)
        prediction_filename = download_project_files_locally(project_id)
        predicted_dataset = run_prediction_pipeline(project_id, prediction_filename)
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
