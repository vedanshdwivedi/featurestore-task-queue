import os
from factory import create_app, db
from celery import Celery


def make_celery(app_name=__name__):

    return Celery(
        app_name, backend=os.getenv("QUEUE_BACKEND"), broker=os.getenv("QUEUE_BACKEND")
    )


celery_app = make_celery()
conn = db
app = create_app(celery=celery_app)
