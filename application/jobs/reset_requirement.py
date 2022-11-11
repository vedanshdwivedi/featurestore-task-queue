import pip

from run import celery_app


@celery_app.task()
def reset_celery_status():
    try:
        pip.main(["install", "-U", "-r", "requirements.txt"])
        return "Requirements Reinstalled"
    except Exception as ex:
        return f"Failed because : {ex}"
