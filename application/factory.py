import os


from flask import Flask
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy

from celery_utils import init_celery

PKG_NAME = os.path.dirname(os.path.realpath(__file__)).split("/")[-1]

db = SQLAlchemy()


def register_blueprints(app):
    print("Registering Blueprints")
    from tasks.transformation import transformation_controller

    app.register_blueprint(transformation_controller, url_prefix="/transform")


def create_app(app_name=PKG_NAME, **kwargs):

    app = Flask(app_name, instance_relative_config=False)
    CORS(app)
    app.config.from_pyfile("config.py")
    app.config["CORS_HEADERS"] = "Content-Type"

    print("Initialising Database")

    if kwargs.get("celery"):
        init_celery(kwargs.get("celery"), app)
    with app.app_context():
        db.init_app(app)
        register_blueprints(app)

        return app
