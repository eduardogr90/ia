from __future__ import annotations

import re
try:
    from slugify import slugify as _vendor_slugify  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    _vendor_slugify = None


def safe_slugify(value: str, fallback: str = "item") -> str:
    """Slugify helper that works even when python-slugify is unavailable."""

    candidate = _vendor_slugify(value) if _vendor_slugify is not None else value
    candidate = candidate.lower()
    candidate = re.sub(r"[^a-z0-9]+", "-", candidate)
    candidate = re.sub(r"-+", "-", candidate).strip("-")
    return candidate or fallback
