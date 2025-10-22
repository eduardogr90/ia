from http import HTTPStatus
from typing import Any, Dict

from flask import jsonify, request
from pydantic import ValidationError

from . import api_bp
from ..core.storage import (
    FlowNotFoundError,
    ProjectNotFoundError,
    Storage,
    get_storage,
)
from ..core.validation import FlowModel

_storage: Storage = get_storage()


def _error_response(message: str, status: int):
    return jsonify({"error": message}), status


@api_bp.get("/projects/<project_id>/flows")
def list_flows(project_id: str):
    try:
        flows = _storage.list_flows(project_id)
    except ProjectNotFoundError:
        return _error_response("Project not found.", HTTPStatus.NOT_FOUND)

    return jsonify({"flows": flows}), HTTPStatus.OK


@api_bp.post("/projects/<project_id>/flows")
def create_flow(project_id: str):
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    if not name:
        return _error_response("Flow name is required.", HTTPStatus.BAD_REQUEST)

    try:
        flow = _storage.create_flow(project_id, name)
    except ProjectNotFoundError:
        return _error_response("Project not found.", HTTPStatus.NOT_FOUND)

    return jsonify(flow), HTTPStatus.CREATED


@api_bp.get("/projects/<project_id>/flows/<flow_id>")
def get_flow(project_id: str, flow_id: str):
    try:
        flow = _storage.load_flow(project_id, flow_id)
    except ProjectNotFoundError:
        return _error_response("Project not found.", HTTPStatus.NOT_FOUND)
    except FlowNotFoundError:
        return _error_response("Flow not found.", HTTPStatus.NOT_FOUND)

    return jsonify(flow), HTTPStatus.OK


@api_bp.put("/projects/<project_id>/flows/<flow_id>")
def save_flow(project_id: str, flow_id: str):
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    try:
        model = FlowModel.model_validate(payload)
    except ValidationError as exc:
        messages = [err["msg"] for err in exc.errors()]
        return jsonify({"valid": False, "errors": messages}), HTTPStatus.BAD_REQUEST

    if model.id != flow_id:
        return _error_response("Flow identifier does not match URL.", HTTPStatus.BAD_REQUEST)

    try:
        flow_dict = model.model_dump(by_alias=True, exclude_none=True)
        saved = _storage.save_flow(project_id, flow_id, flow_dict)
    except ProjectNotFoundError:
        return _error_response("Project not found.", HTTPStatus.NOT_FOUND)
    except FlowNotFoundError:
        return _error_response("Flow not found.", HTTPStatus.NOT_FOUND)

    return jsonify(saved), HTTPStatus.OK


@api_bp.delete("/projects/<project_id>/flows/<flow_id>")
def delete_flow(project_id: str, flow_id: str):
    try:
        _storage.delete_flow(project_id, flow_id)
    except ProjectNotFoundError:
        return _error_response("Project not found.", HTTPStatus.NOT_FOUND)
    except FlowNotFoundError:
        return _error_response("Flow not found.", HTTPStatus.NOT_FOUND)

    return "", HTTPStatus.NO_CONTENT
