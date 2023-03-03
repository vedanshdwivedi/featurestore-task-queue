from utilities.utils import *

mongoClient = get_mongo_client()
engine = get_db_engine()
blobClient = get_blob_client()


def handle_existing_file_metadata_delete_by_category(projectId: int, category: str):
    query = f"""UPDATE files SET deleted = {True} WHERE pid = {projectId} AND category = '{category}' """
    try:
        engine.execute(query)
    except Exception as ex:
        raise Exception(f"Unable to delete existing file entries : {ex}")


def create_file_entry_in_postgres(projectId: int, filename: str, container: str, downloadLink: str, category: str) -> \
        None:
    downloadLink = downloadLink.strip("'")
    query = f"""INSERT INTO files ("pid", "filename", "container", "downloadLink", "category") VALUES ({projectId}, '{filename}', '{container}', '{downloadLink}', '{category}'
    )"""
    try:
        engine.execute(query)
    except Exception as ex:
        raise Exception(f"FAILED TO WRITE FILE METADATA IN POSTGRES : {ex}")
