from flask import Blueprint

api_bp = Blueprint("api", __name__, url_prefix="/api")

from . import projects, flows, validate, export  # noqa: F401,E402

__all__ = ["api_bp"]
