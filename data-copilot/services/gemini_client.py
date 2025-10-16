"""Utilities to initialise the Gemini model via Vertex AI."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

from crewai import LLM
from google.oauth2 import service_account
from langchain_google_vertexai import GoogleGenAI

DEFAULT_VERTEX_LOCATION = "us-central1"
DEFAULT_GEMINI_MODEL = "gemini-2.0-flash-lite-001"
LOGGER = logging.getLogger(__name__)


def load_vertex_credentials(path: str = "config/json_key_vertex.json") -> Dict[str, Any]:
    """Load a service account key stored locally."""

    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError as exc:
        LOGGER.error("No se encontró el archivo de credenciales de Vertex AI: %s", path)
        raise
    except json.JSONDecodeError as exc:  # pragma: no cover - depends on local file
        LOGGER.error("El archivo de credenciales de Vertex AI está corrupto: %s", path)
        raise ValueError("Credenciales de Vertex AI inválidas") from exc


class VertexAIGeminiLLM(LLM):
    """Thin wrapper that exposes a Vertex AI Gemini model as a CrewAI ``LLM``."""

    def __init__(
        self,
        client: GoogleGenAI,
        *,
        model: str,
        temperature: float,
        project_id: str,
        location: str,
    ) -> None:
        try:
            super().__init__(
                model=model,
                temperature=temperature,
                provider="vertex_ai",
            )
        except TypeError:
            # Older CrewAI releases ignore provider/max tokens parameters.
            super().__init__(model=model, temperature=temperature)
        self._client = client
        self.project_id = project_id
        self.location = location

    # CrewAI agents typically invoke ``llm.call`` or ``llm.invoke`` when available.
    def invoke(self, prompt: str, **kwargs: Any) -> Any:
        """Proxy the call to the underlying LangChain Vertex AI client."""

        response = self._client.invoke(prompt, **kwargs)
        return self._normalise_response(response)

    def call(self, prompt: str, **kwargs: Any) -> Any:  # pragma: no cover - alias for CrewAI
        """Compatibility alias used by some CrewAI internals."""

        return self.invoke(prompt, **kwargs)

    def __call__(self, prompt: str, **kwargs: Any) -> Any:  # pragma: no cover - alias for LangChain
        """Allow the wrapper to be used like a plain callable LLM."""

        return self.invoke(prompt, **kwargs)

    @staticmethod
    def _normalise_response(response: Any) -> Any:
        if isinstance(response, str):
            return response
        text = getattr(response, "text", None)
        if text:
            return text
        if isinstance(response, dict):
            return response.get("text") or response.get("output") or response
        return response

    def as_langchain(self) -> GoogleGenAI:
        """Expose the underlying LangChain client when direct access is required."""

        return self._client


def init_gemini_llm(
    json_key: Dict[str, Any],
    project_id: str,
    location: str | None = None,
    *,
    model: str | None = None,
    temperature: float = 0.1,
    max_output_tokens: int = 2048,
) -> LLM:
    """Initialise a Gemini model using Vertex AI and expose it as a CrewAI LLM."""

    if not project_id:
        raise ValueError("project_id es requerido para inicializar Gemini")

    vertex_location = location or os.getenv("VERTEX_LOCATION", DEFAULT_VERTEX_LOCATION)
    model_name = model or os.getenv("VERTEX_MODEL", DEFAULT_GEMINI_MODEL)

    try:
        credentials = service_account.Credentials.from_service_account_info(
            json_key,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    except Exception as exc:  # pragma: no cover - depends on runtime secrets
        LOGGER.exception("No se pudieron construir las credenciales de Vertex AI")
        raise RuntimeError("No se pudieron construir las credenciales de Vertex AI") from exc

    vertexai_config = {
        "project": project_id,
        "location": vertex_location,
        "credentials": credentials,
    }

    try:
        langchain_client = GoogleGenAI(
            model=model_name,
            temperature=temperature,
            max_output_tokens=max_output_tokens,
            vertexai_config=vertexai_config,
        )
    except Exception as exc:  # pragma: no cover - depends on runtime env
        LOGGER.exception("No se pudo inicializar el cliente de Vertex AI Gemini")
        raise RuntimeError("No se pudo inicializar el cliente de Vertex AI Gemini") from exc

    return VertexAIGeminiLLM(
        langchain_client,
        model=model_name,
        temperature=temperature,
        project_id=project_id,
        location=vertex_location,
    )


__all__ = ["load_vertex_credentials", "init_gemini_llm", "VertexAIGeminiLLM"]
