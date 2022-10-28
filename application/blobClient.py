import os

from azure.storage.blob import BlobServiceClient


class BlobConnection:
    __instance = None
    blob_client = None

    @staticmethod
    def getInstance():
        if BlobConnection.__instance is None:
            BlobConnection()
        return BlobConnection.__instance

    def __init__(self):
        if BlobConnection.__instance is not None:
            raise Exception("This is a singleton class")
        else:
            connection_string = os.getenv("AZURE_CONNECTION_STRING")
            self.blob_client = BlobServiceClient.from_connection_string(
                connection_string
            )
            BlobConnection.__instance = self

    def get_engine(self):
        if self.blob_client is None:
            raise Exception("Blob Client Not Initialized")
        return self.blob_client
