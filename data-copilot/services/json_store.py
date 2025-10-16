"""Utility helpers for working with JSON files on disk."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def load_json(path: Path) -> Dict[str, Any]:
    """Load JSON data from *path*.

    Returns an empty dictionary when the file does not exist or is empty.
    """
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError:
        # If the JSON file is corrupt we return an empty structure to avoid crashing.
        return {}


def save_json(path: Path, data: Dict[str, Any]) -> None:
    """Persist *data* to *path* ensuring parent directories exist."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
