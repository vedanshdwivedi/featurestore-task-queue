import os
import ssl
from typing import Dict

from azure.storage.blob import BlobServiceClient

from factory import create_app
from celery import Celery
import yaml


def get_backend_and_broker_url() -> Dict[str, str]:
    response_dict = {"BROKER": "", "BACKEND": ""}
    with open("config_vars.yaml", "r") as file:
        config = yaml.safe_load(file)
        response_dict["BROKER"] = config["config"]["QUEUE"]["BROKER_URI"]
        response_dict["BACKEND"] = config["config"]["QUEUE"]["BACKEND_URI"]
    return response_dict


def make_celery(app_name=__name__):
    print("Initialising Celery")
    resp = get_backend_and_broker_url()
    backend, broker = resp["BACKEND"], resp["BROKER"]
    return Celery(
        app_name,
        backend=backend,
        broker=broker,
        broker_use_ssl={"ssl_cert_reqs": ssl.CERT_NONE},
        redis_backend_use_ssl={"ssl_cert_reqs": ssl.CERT_NONE},
    )





celery_app = make_celery()
print("Connecting Celery to Flask")
app = create_app(celery=celery_app)
print("Celery connected to Flask Successfully")
