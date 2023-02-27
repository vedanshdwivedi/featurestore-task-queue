import os
from datetime import datetime

from flask import Flask
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from segment import analytics
from flask_pymongo import PyMongo

from celery_utils import init_celery

PKG_NAME = os.path.dirname(os.path.realpath(__file__)).split("/")[-1]

db = SQLAlchemy()
mongo = PyMongo()

def register_blueprints(app):
    print("Registering Blueprints")
    from tasks.xgbregressor import xgboost_controller

    app.register_blueprint(xgboost_controller, url_prefix="/xgbregressor")


def create_app(app_name=PKG_NAME, **kwargs):

    app = Flask(app_name, instance_relative_config=False)
    CORS(app)
    app.config.from_pyfile("config.py")
    app.config["CORS_HEADERS"] = "Content-Type"
    analytics.write_key = os.getenv("SEGMENT_KEY")
    analytics.debug = True
    analytics.identify(user_id='123', traits={'email': 'vedanshdwivedi0@gmail.com'})

    print("Initialising Database")

    if kwargs.get("celery"):
        init_celery(kwargs.get("celery"), app)
    with app.app_context():
        db.init_app(app)
        mongo.init_app(app)
        register_blueprints(app)

        return app
