from flask_sqlalchemy import SQLAlchemy
from flask import Flask
import os

from celery_utils import init_celery

PKG_NAME = os.path.dirname(os.path.realpath(__file__)).split("/")[-1]

db = SQLAlchemy()


def register_blueprints(app):
    from all import bp
    from tasks.transformation import transformation_controller

    app.register_blueprint(bp)
    app.register_blueprint(transformation_controller, url_prefix="/transform")


def create_app(app_name=PKG_NAME, **kwargs):

    app = Flask(app_name)
    app.config.from_pyfile("config.py")

    db.init_app(app)
    if kwargs.get("celery"):
        init_celery(kwargs.get("celery"), app)
    register_blueprints(app)
    return app
