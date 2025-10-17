"""Application configuration settings."""
import os
from pathlib import Path

# Base directory for application
BASE_DIR = Path(__file__).resolve().parent.parent

# Secret key for session management. In production this should be set via env var.
SECRET_KEY = os.environ.get("DATA_COPILOT_SECRET", "super-secret-key-change-me")


def _get_float_env(var_name: str, default: float) -> float:
    """Read a floating point value from the environment.

    Returns the provided default when the variable is unset or invalid.
    """

    raw_value = os.environ.get(var_name)
    if raw_value is None:
        return default
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return default

# Paths to data directories
DATA_DIR = BASE_DIR / "data"
USERS_FILE = DATA_DIR / "users.json"
CONVERSATIONS_DIR = DATA_DIR / "conversations"

# Ensure the conversations directory exists when the app starts.
CONVERSATIONS_DIR.mkdir(parents=True, exist_ok=True)

# Optional pricing information used to estimate LLM costs in the logs.
GEMINI_PROMPT_COST_PER_1K = _get_float_env("GEMINI_PROMPT_COST_PER_1K", 0.0)
GEMINI_COMPLETION_COST_PER_1K = _get_float_env("GEMINI_COMPLETION_COST_PER_1K", 0.0)
