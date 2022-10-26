# featurestore-task-queue
Async Task Queue based on celery

Project Requirements:

- Task Queue [Use Redis, or Kafka, or RabbitMQ]
- To start the celery server, use command :
  - cd application
  -  celery -A run.celery_app worker --loglevel=INFO --pool=solo --concurrency=2
- To start Flask Server
  - cd application
  - python3 wsgi.py
- To start flower
  - cd application
  - celery -A run.celery_app flower
