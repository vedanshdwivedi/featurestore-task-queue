from flask import Blueprint, request

transformation_controller = Blueprint("transform", __name__)


@transformation_controller.route("/predict", methods=["POST"])
def predict_outputs():
    pass
