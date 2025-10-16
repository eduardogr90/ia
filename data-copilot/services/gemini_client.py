"""Utilities to initialise the Gemini model via Vertex AI."""
from __future__ import annotations

import json
import os
from typing import Any, Dict

from crewai import Settings
from google.oauth2 import service_account
from langchain_google_vertexai import GoogleGenAI

DEFAULT_VERTEX_LOCATION = "us-central1"


def load_vertex_credentials(path: str = "config/json_key_vertex.json") -> Dict[str, Any]:
    """Load a service account key stored locally."""

    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def init_gemini_llm(
    json_key: Dict[str, Any],
    project_id: str,
    location: str | None = None,
) -> GoogleGenAI:
    """Initialise a Gemini model using Vertex AI and register it in CrewAI settings."""

    if not project_id:
        raise ValueError("project_id es requerido para inicializar Gemini")

    vertex_location = location or os.getenv("VERTEX_LOCATION", DEFAULT_VERTEX_LOCATION)
    credentials = service_account.Credentials.from_service_account_info(
        json_key,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    llm = GoogleGenAI(
        model="gemini-2.0-flash-lite-001",
        temperature=0.1,
        max_output_tokens=2048,
        vertexai_config={
            "project": project_id,
            "location": vertex_location,
            "credentials": credentials,
        },
    )

    Settings.llm = llm
    return llm


__all__ = ["load_vertex_credentials", "init_gemini_llm"]
