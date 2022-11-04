python3 -m venv ./venv

source venv/bin/activate
pip install -r requirements.txt

cd application
celery -A celery_worker.celery_app worker --loglevel=INFO --pool=solo --concurrency=2