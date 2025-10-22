from http import HTTPStatus
from typing import Any, Dict, List

from flask import jsonify, request
from pydantic import ValidationError

from . import api_bp
from ..core.validation import FlowModel, enumerate_paths, validate_flow


def _format_validation_errors(errors: List[Dict[str, Any]]) -> List[str]:
    formatted = []
    for error in errors:
        location = ".".join(str(part) for part in error.get("loc", ()))
        if location:
            formatted.append(f"{location}: {error.get('msg')}")
        else:
            formatted.append(str(error.get("msg")))
    return formatted


@api_bp.post("/validate")
def validate_flow_endpoint():
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    try:
        model = FlowModel.model_validate(payload)
    except ValidationError as exc:
        errors = _format_validation_errors(exc.errors())
        return (
            jsonify({"valid": False, "errors": errors, "warnings": [], "paths": []}),
            HTTPStatus.BAD_REQUEST,
        )

    valid, errors, warnings = validate_flow(model)
    paths = enumerate_paths(model)

    response = {
        "valid": valid,
        "errors": errors,
        "warnings": warnings,
        "paths": paths,
    }
    return jsonify(response), HTTPStatus.OK
