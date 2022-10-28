import os

import yaml


def get_config_vars():
    print("Initialising Configuration")
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
os.environ["AZURE_CONNECTION_STRING"] = config_vars["config"]["AZURE"][
    "CONNECTION_STRING"
]
os.environ["SECRET_KEY"] = config_vars["config"]["SECRET_KEY"]
os.environ["LOCAL_FILE_PATH"] = config_vars["config"]["LOCAL_FILE_PATH"]
