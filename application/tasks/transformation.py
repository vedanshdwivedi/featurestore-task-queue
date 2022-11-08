from flask import Blueprint, request, make_response, jsonify

from jobs.transformation.transform import run_prediction_jobs

transformation_controller = Blueprint("transform", __name__)


@transformation_controller.route("/predict", methods=["POST"])
def predict_outputs():
    data = request.json
    try:
        project_id = int(data["project_id"])
        run_prediction_jobs.delay(project_id=project_id)
        return make_response(jsonify({"task_response": "Task Started"}), 200)
    except Exception as ex:
        return make_response(
            jsonify({"task_response": f"Error in executing : {ex}"}), 400
        )
