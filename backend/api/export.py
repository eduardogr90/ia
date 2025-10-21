from http import HTTPStatus
from typing import Any, Dict, List

from flask import jsonify, request
from pydantic import ValidationError
from . import api_bp
from ..core.validation import FlowModel
from ..core.utils import safe_slugify
from ..core.yaml_export import to_yaml


def _format_validation_errors(errors: List[Dict[str, Any]]) -> List[str]:
    formatted = []
    for error in errors:
        location = ".".join(str(part) for part in error.get("loc", ()))
        if location:
            formatted.append(f"{location}: {error.get('msg')}")
        else:
            formatted.append(str(error.get("msg")))
    return formatted


@api_bp.post("/export/yaml")
def export_flow_to_yaml():
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    try:
        model = FlowModel.model_validate(payload)
    except ValidationError as exc:
        errors = _format_validation_errors(exc.errors())
        return jsonify({"errors": errors}), HTTPStatus.BAD_REQUEST

    yaml_text = to_yaml(model)
    slug = safe_slugify(model.name or model.id or "flow", fallback="flow")
    filename = f"{slug}.yaml"

    return jsonify({"yaml": yaml_text, "filename": filename}), HTTPStatus.OK
