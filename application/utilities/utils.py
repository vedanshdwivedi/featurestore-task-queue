import base64
import hashlib
import hmac
import os
import os.path
import re
import shutil
from importlib.machinery import SourceFileLoader
from typing import Dict, Any

from jobs.TransformationBase import TransformationBase

import pandas as pd
from azure.storage.blob import BlobServiceClient

from blobClient import BlobConnection
from db import DBConnection


def hash_string_using_secret_key(string_to_hash: str) -> str:
    salt = os.getenv("SECRET_KEY")
    dig = hmac.new(
        bytes(salt, "utf-8"), bytes(string_to_hash, "utf-8"), digestmod=hashlib.sha256
    ).digest()
    key = base64.b64encode(dig).decode()
    key = re.sub("\W+", "", key).lower()
    return key


def get_db_engine():
    dbcon = DBConnection.getInstance()
    return dbcon.get_engine()


def get_mongo_client():
    dbcon = DBConnection.getInstance()
    return dbcon.get_mongo_engine()


def get_blob_client() -> BlobServiceClient:
    blobcon = BlobConnection.getInstance()
    return blobcon.blob_client


def download_project_data_locally(project_id: int) -> None:
    create_project_dir(project_id)


def create_project_dir(project_id: int) -> None:
    project_path = os.path.join(
        os.environ.get("LOCAL_FILE_PATH", "files"), f"project-{project_id}"
    )
    if not os.path.exists(project_path):
        os.mkdir(project_path)


def del_project_dir(project_id: int) -> None:
    project_path = os.path.join(
        os.environ.get("LOCAL_FILE_PATH", "files"), f"project-{project_id}"
    )
    if os.path.exists(project_path):
        shutil.rmtree(project_path)


def get_initialised_tranformation_object(project_id: int, dataset_path: str):
    project_folder = os.path.join(
        os.environ.get("LOCAL_FILE_PATH", "files"), f"project-{project_id}"
    )
    transformation_file_path = os.path.join(project_folder, "transformation.py")
    try:
        transformation_object = (
            SourceFileLoader("transformation", transformation_file_path)
            .load_module()
            .Transformation(dataset_path)
        )
        tclass = TransformationBase(transformation_object)
        return tclass
    except Exception as ex:
        raise Exception(
            f"[utils][get_initialised_tranformation_object] Unable to initialize Tranformation Class : {ex}"
        )


def download_blob_locally(project_id: int, file_name: str, container_name: str) -> str:
    if project_id is None or file_name is None or container_name is None:
        raise ValueError(
            "[UTILS][get_transformation_file] project_id, file_name or container name can't be none"
        )
    blob_service = get_blob_client()
    create_project_dir(project_id)

    local_file_directory = os.environ.get("LOCAL_FILE_PATH", "files")
    blob_client_instance = blob_service.get_blob_client(
        container=container_name, blob=file_name
    )
    download_path = os.path.join(
        local_file_directory, f"project-{project_id}", file_name
    )
    try:
        if not os.path.exists(local_file_directory):
            os.mkdir(local_file_directory)
        bytes = blob_client_instance.download_blob().readall()
        with open(download_path, "wb") as file:
            file.write(bytes)
        return download_path
    except Exception as ex:
        raise Exception(ex)


def save_dataset_locally(df: pd.DataFrame, project_id: int, filename: str) -> None:
    project_folder = os.path.join(
        os.environ.get("LOCAL_FILE_PATH", "files"), f"project-{project_id}"
    )
    file_path = os.path.join(project_folder, filename)
    if not os.path.exists(project_folder):
        os.mkdir(project_folder)
    try:
        df.to_excel(file_path)
    except Exception as ex:
        raise Exception(
            f"[UTILS][save_dataset_locally] Unable to save dataframe as xlsx : {ex}"
        )


def upload_dataset_to_blob(dataset_path: str, container: str) -> str:
    blob_client = get_blob_client()
    uploader = blob_client.get_blob_client(container=container, blob=dataset_path)
    with open(dataset_path, "rb") as data:
        uploader.upload_blob(data, overwrite=True)
        url = uploader.url
    return url


def delete_file(filename: str) -> None:
    pass


def get_testing_dataset():
    pass


def get_training_dataset():
    pass
