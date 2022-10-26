import os

import yaml


def get_config_vars():
    filepath = "config_vars.yaml"
    with open(filepath, "r") as file:
        try:
            config_vars = yaml.safe_load(file)
            return config_vars
        except Exception as ex:
            raise Exception(f"Unable to read config.yaml : {ex}")


config_vars = get_config_vars()

SQLALCHEMY_DATABASE_URI = config_vars["config"]["POSTGRES"]["URI"]
AZURE = {"CONNECTION_STRING": config_vars["config"]["AZURE"]["CONNECTION_STRING"]}
BROKER_URL = config_vars["config"]["QUEUE"]["BROKER_URI"]
BACKEND_URL = config_vars["config"]["QUEUE"]["BACKEND_URI"]
os.environ["AZURE_CONNECTION_STRING"] = config_vars["config"]["AZURE"][
    "CONNECTION_STRING"
]
os.environ["SECRET_KEY"] = config_vars["config"]["SECRET_KEY"]
os.environ["QUEUE_BROKER"] = BROKER_URL
os.environ["QUEUE_BACKEND"] = BACKEND_URL
