from flask import Blueprint, request
from jobs.transformation.transform import create_predictions

transformation_controller = Blueprint("transform", __name__)


@transformation_controller.route("/predict", methods=["POST"])
def predict_outputs():
    data = request.json()
    try:
        project_id = data["project_id"]
        file_id = data["file_id"]
        create_predictions(project_id=project_id, file_id=file_id)
    except Exception as ex:
        raise Exception(ex)
