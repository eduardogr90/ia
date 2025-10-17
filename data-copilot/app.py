"""Entry point for the Data Copilot Flask application."""
from __future__ import annotations

import logging
from functools import wraps
from typing import Callable, Dict, Optional

from flask import (
    Flask,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from config import settings
from crew.orchestrator import OrchestrationError, OrchestrationResult, get_orchestrator
from services.auth import auth_service
from services.conversation_service import conversation_service

LOGGER = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = settings.SECRET_KEY


def login_required(view: Callable) -> Callable:
    """Decorator to ensure the user is authenticated."""

    @wraps(view)
    def wrapped(*args, **kwargs):
        if "username" not in session:
            if request.accept_mimetypes.accept_json and not request.accept_mimetypes.accept_html:
                return jsonify({"error": "authentication_required"}), 401
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped


@app.route("/", methods=["GET", "POST"])
def login():
    """Render the login page and authenticate users."""
    error: Optional[str] = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if auth_service.authenticate(username, password):
            session["username"] = username
            return redirect(url_for("chat"))
        error = "Credenciales inválidas"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.pop("username", None)
    return redirect(url_for("login"))


@app.route("/chat")
@login_required
def chat():
    username = session["username"]
    conversations = [conv.to_dict() for conv in conversation_service.list_conversations(username)]
    return render_template("chat.html", username=username, conversations=conversations)


@app.route("/new_chat", methods=["POST"])
@login_required
def new_chat():
    username = session["username"]
    conversation = conversation_service.create_conversation(username)
    return jsonify(conversation.to_dict())


@app.route("/load_chat/<conv_id>")
@login_required
def load_chat(conv_id: str):
    username = session["username"]
    conversation = conversation_service.load_conversation(username, conv_id)
    if not conversation:
        return jsonify({"error": "conversation_not_found"}), 404
    return jsonify(conversation.to_dict())


@app.route("/send_message", methods=["POST"])
@login_required
def send_message():
    username = session["username"]
    payload: Dict[str, str] = request.get_json(force=True)
    conv_id = payload.get("conversation_id")
    message = payload.get("message", "").strip()
    if not conv_id or not message:
        return jsonify({"error": "invalid_payload"}), 400

    conversation = conversation_service.append_message(username, conv_id, "user", message)
    if not conversation:
        return jsonify({"error": "conversation_not_found"}), 404

    orchestration: Optional[OrchestrationResult] = None
    try:
        orchestrator = get_orchestrator()
        orchestration = orchestrator.handle_message(message, conversation.messages)
        assistant_reply = orchestration.response
    except OrchestrationError as exc:
        LOGGER.error("Error durante la orquestación de agentes", exc_info=True)
        assistant_reply = str(exc)
    except Exception as exc:  # pragma: no cover - defensive safeguard
        LOGGER.exception("Excepción no controlada al procesar el mensaje")
        assistant_reply = f"{exc.__class__.__name__}: {exc}"

    chart_payload = orchestration.chart if orchestration else None
    conversation = conversation_service.append_message(
        username,
        conv_id,
        "assistant",
        assistant_reply,
        extra={"chart": chart_payload} if chart_payload else None,
    )
    response_payload: Dict[str, object] = {"response": assistant_reply}
    if chart_payload:
        response_payload["chart"] = chart_payload
    if conversation:
        response_payload["conversation"] = conversation.to_dict()
    if orchestration:
        response_payload["metadata"] = {
            "sql": orchestration.sql,
            "rows": orchestration.rows,
            "error": orchestration.error,
            "interpreter": orchestration.interpreter_output,
            "sql_output": orchestration.sql_output,
            "validation": orchestration.validation_output,
            "analysis": orchestration.analyzer_output,
        }
    return jsonify(response_payload)


@app.route("/delete_chat/<conv_id>", methods=["DELETE"])
@login_required
def delete_chat(conv_id: str):
    username = session["username"]
    removed = conversation_service.delete_conversation(username, conv_id)
    if not removed:
        return jsonify({"error": "conversation_not_found"}), 404
    return jsonify({"status": "deleted"})


if __name__ == "__main__":
    app.run(debug=True)
