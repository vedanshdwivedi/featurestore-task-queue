import os.path

from run import db, blob_service


def get_transformation_file(file_name: str, container_name: str) -> str:
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
