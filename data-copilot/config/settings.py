"""Application configuration settings."""
import os
from pathlib import Path

# Base directory for application
BASE_DIR = Path(__file__).resolve().parent.parent

# Secret key for session management. In production this should be set via env var.
SECRET_KEY = os.environ.get("DATA_COPILOT_SECRET", "super-secret-key-change-me")

# Paths to data directories
DATA_DIR = BASE_DIR / "data"
USERS_FILE = DATA_DIR / "users.json"
CONVERSATIONS_DIR = DATA_DIR / "conversations"

# Ensure the conversations directory exists when the app starts.
CONVERSATIONS_DIR.mkdir(parents=True, exist_ok=True)
