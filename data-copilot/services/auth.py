"""Authentication helpers."""
from __future__ import annotations

from typing import Dict, Optional

from config import settings
from services.json_store import load_json


class AuthService:
    """Simple user authentication based on a JSON file."""

    def __init__(self) -> None:
        # Cache the initial user map, but always allow reloading to pick up
        # password edits made directly to the JSON file.
        self._users: Dict[str, Dict[str, str]] = load_json(settings.USERS_FILE)

    def _reload(self) -> None:
        """Reload user data from disk to ensure latest credentials."""
        self._users = load_json(settings.USERS_FILE)

    def authenticate(self, username: str, password: str) -> bool:
        """Validate *username* and *password* against the stored values."""
        # Always refresh the user list so changes to the JSON file take effect
        # without restarting the application.
        self._reload()
        user = self._users.get(username)
        if not user:
            return False
        stored_password: Optional[str] = user.get("password")
        if stored_password is None:
            return False
        return stored_password == password


auth_service = AuthService()
