from utilities.utils import *

mongoClient = get_mongo_client()
engine = get_db_engine()
blobClient = get_blob_client()


def create_file_entry_in_postgres(projectId: int, filename: str, container: str, downloadLink: str, category: str) -> \
        None:
    query = f"""INSERT INTO files (pid, filename, container, downloadLink, category) VALUES ({projectId}, '{filename}', '{container}', '{downloadLink}', '{category}'
    )"""
    try:
        engine.execute(query)
    except Exception as ex:
        raise Exception("FAILED TO WRITE FILE METADATA IN POSTGRES")
