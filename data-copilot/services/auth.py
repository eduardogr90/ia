"""Authentication helpers."""
from __future__ import annotations

from typing import Optional

from werkzeug.security import check_password_hash

from config import settings
from services.json_store import load_json


class AuthService:
    """Simple user authentication based on a JSON file."""

    def __init__(self) -> None:
        self._users = load_json(settings.USERS_FILE)

    def authenticate(self, username: str, password: str) -> bool:
        """Validate *username* and *password* against the stored hashes."""
        user = self._users.get(username)
        if not user:
            return False
        stored_hash: Optional[str] = user.get("password")
        if not stored_hash:
            return False
        return check_password_hash(stored_hash, password)


auth_service = AuthService()
