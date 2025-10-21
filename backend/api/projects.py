from http import HTTPStatus
from typing import Any, Dict

from flask import jsonify, request

from . import api_bp
from ..core.storage import (
    ProjectNotFoundError,
    Storage,
    get_storage,
)

_storage: Storage = get_storage()


def _error_response(message: str, status: int):
    return jsonify({"error": message}), status


@api_bp.get("/projects")
def list_projects():
    projects = _storage.list_projects()
    return jsonify({"projects": projects}), HTTPStatus.OK


@api_bp.post("/projects")
def create_project():
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    if not name:
        return _error_response("Project name is required.", HTTPStatus.BAD_REQUEST)

    project = _storage.create_project(name)
    return jsonify(project), HTTPStatus.CREATED


@api_bp.patch("/projects/<project_id>")
def rename_project(project_id: str):
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    name = payload.get("name")
    if name is not None:
        name = name.strip()
    if not name:
        return _error_response("Project name is required to rename a project.", HTTPStatus.BAD_REQUEST)

    try:
        project = _storage.rename_project(project_id, name)
    except ProjectNotFoundError:
        return _error_response("Project not found.", HTTPStatus.NOT_FOUND)

    return jsonify(project), HTTPStatus.OK


@api_bp.delete("/projects/<project_id>")
def delete_project(project_id: str):
    try:
        _storage.delete_project(project_id)
    except ProjectNotFoundError:
        return _error_response("Project not found.", HTTPStatus.NOT_FOUND)

    return "", HTTPStatus.NO_CONTENT
