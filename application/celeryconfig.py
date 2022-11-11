from celery.schedules import crontab

CELERY_IMPORTS = ("jobs.transformation.transform", "jobs.reset_requirement")
CELERY_TASK_RESULT_EXPIRES = 30
CELERY_TIMEZONE = "UTC"

CELERY_ACCEPT_CONTENT = ["json", "msgpack", "yaml"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"

CELERYBEAT_SCHEDULE = {
    "reset_celery_dependency": {
        "task": "jobs.reset_requirement.reset_celery_status",
        "schedule": crontab(minute="30"),
    }
}
