from celery_utils import init_celery
from factory import create_app
from run import celery_app

app = create_app()
init_celery(celery_app, app)