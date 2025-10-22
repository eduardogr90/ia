"""Tests for Gemini client helpers."""

from __future__ import annotations

import sys
from pathlib import Path

# Allow importing ``services.gemini_client`` when running tests from repo root.
sys.path.append(str(Path(__file__).resolve().parents[2]))

from services.gemini_client import _ensure_crewai_llm_compatibility


class _DummyLLM:
    """Minimal stub that mimics the VertexAI interface used by the app."""

    def invoke(self, prompt: str) -> str:  # pragma: no cover - defensive
        return prompt


class _FrozenLLM(_DummyLLM):
    """LLM stub that forbids setting new attributes, similar to Pydantic models."""

    def __init__(self) -> None:
        object.__setattr__(self, "_frozen", True)

    def __setattr__(self, name: str, value: object) -> None:  # pragma: no cover - behavior under test
        raise ValueError(f"Cannot set attribute {name}")


def test_ensure_crewai_llm_adds_supports_stop_words_when_missing() -> None:
    llm = _DummyLLM()

    assert not hasattr(llm, "supports_stop_words")

    result = _ensure_crewai_llm_compatibility(llm)

    assert result is llm
    assert hasattr(llm, "supports_stop_words")
    assert callable(llm.supports_stop_words)
    assert llm.supports_stop_words() is False


def test_ensure_crewai_llm_preserves_existing_method() -> None:
    class _CustomLLM(_DummyLLM):
        def supports_stop_words(self) -> bool:  # pragma: no cover - trivial
            return True

    llm = _CustomLLM()
    original_method = llm.supports_stop_words

    result = _ensure_crewai_llm_compatibility(llm)

    assert result is llm
    assert llm.supports_stop_words.__func__ is original_method.__func__
    assert llm.supports_stop_words() is True


def test_ensure_crewai_llm_wraps_when_attribute_assignment_forbidden() -> None:
    llm = _FrozenLLM()

    result = _ensure_crewai_llm_compatibility(llm)

    assert result is not llm
    assert hasattr(result, "supports_stop_words")
    assert result.supports_stop_words() is False
    assert result.invoke("hola") == "hola"
