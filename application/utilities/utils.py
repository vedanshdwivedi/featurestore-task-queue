import os.path

from azure.storage.blob import BlobServiceClient

from blobClient import BlobConnection
from db import DBConnection


def get_db_engine():
    dbcon = DBConnection.getInstance()
    return dbcon.get_engine()


def get_blob_client() -> BlobServiceClient:
    blobcon = BlobConnection.getInstance()
    return blobcon.blob_client


def get_transformation_file(file_name: str, container_name: str) -> str:
    blob_service = get_blob_client()

    local_file_directory = os.environ.get("LOCAL_FILE_PATH", "files")
    blob_client_instance = blob_service.get_blob_client(
        container=container_name, blob=file_name
    )
    download_path = os.path.join(local_file_directory, file_name)
    if not os.path.exists(local_file_directory):
        os.mkdir(local_file_directory)
    bytes = blob_client_instance.download_blob().readall()
    with open(download_path, "wb") as file:
        file.write(bytes)
    return download_path


def delete_file(filename: str) -> None:
    pass


def get_testing_dataset():
    pass


def get_training_dataset():
    pass
