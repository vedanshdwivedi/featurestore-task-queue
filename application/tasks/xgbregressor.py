import json

from flask import Blueprint, request, make_response, jsonify
from jobs.xgboostregressor.xgbregressor import XGBRegressorPipeline

xgboost_controller = Blueprint("transform", __name__)


@xgboost_controller.route("/train", methods=["POST"])
def train():
    try:
        print("XGB Regressor Pipeline Training")
        data = request.json
        if type(data) == str:
            data = json.loads(data)
        project_id = int(data["projectId"])
        pipeline = XGBRegressorPipeline(project_id)
        pipeline.trainModel()
        # run_prediction_jobs.delay(project_id=project_id, request_id=task_id)
        return make_response(jsonify({"task response": f"Training Task Started", "data": data}), 200)
    except Exception as ex:
        return make_response(
            jsonify({"task_response": f"Error in executing : {ex}"}), 400
        )


@xgboost_controller.route("/predict", methods=["POST"])
def predict():
    try:
        print("XGB Regressor Pipeline Predicting")
        # run_prediction_jobs.delay(project_id=project_id, request_id=task_id)
        return make_response(jsonify({"task response": "Prediction Task Started"}), 200)
    except Exception as ex:
        return make_response(
            jsonify({"task_response": f"Error in executing : {ex}"}), 400
        )
