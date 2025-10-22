import os

from flask import Flask
from flask_cors import CORS

from .api import api_bp


def create_app() -> Flask:
    app = Flask(__name__)
    CORS(app)
    app.register_blueprint(api_bp)
    return app


if __name__ == "__main__":
    application = create_app()
    port = int(os.environ.get("PORT", "8000"))
    application.run(host="0.0.0.0", port=port)
