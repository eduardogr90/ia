"""Cliente utilitario para inicializar modelos Gemini de Vertex AI."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping

from google.oauth2 import service_account
from langchain_google_vertexai import VertexAI

LOGGER = logging.getLogger(__name__)

DEFAULT_VERTEX_LOCATION = "us-central1"
DEFAULT_GEMINI_MODEL = "gemini-1.5-pro"
DEFAULT_CREDENTIALS_PATH = Path("config/json_key_vertex.json")
VERTEX_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)


def _build_credentials_from_info(info: Mapping[str, Any]) -> service_account.Credentials:
    """Construye credenciales de servicio a partir de un diccionario JSON."""

    try:
        credentials = service_account.Credentials.from_service_account_info(
            info,
            scopes=VERTEX_SCOPES,
        )
    except Exception as exc:  # pragma: no cover - depende de los secretos reales
        LOGGER.error("No se pudieron construir las credenciales desde el JSON proporcionado")
        raise ValueError("Credenciales de Vertex AI inválidas") from exc
    return credentials


def _build_credentials_from_file(path: Path) -> service_account.Credentials:
    """Carga credenciales de servicio desde un archivo JSON."""

    try:
        credentials = service_account.Credentials.from_service_account_file(
            path,
            scopes=VERTEX_SCOPES,
        )
    except FileNotFoundError as exc:
        LOGGER.error("No se encontró el archivo de credenciales de Vertex AI: %s", path)
        raise
    except Exception as exc:  # pragma: no cover - depende de archivos corruptos
        LOGGER.error("El archivo de credenciales de Vertex AI está corrupto: %s", path)
        raise ValueError("Credenciales de Vertex AI inválidas") from exc
    return credentials


def load_vertex_credentials(
    path_env_var: str = "GOOGLE_APPLICATION_CREDENTIALS",
    *,
    credentials_path: str | os.PathLike[str] | None = None,
    json_credentials: Mapping[str, Any] | None = None,
    default_path: str | os.PathLike[str] | None = DEFAULT_CREDENTIALS_PATH,
) -> service_account.Credentials:
    """Carga las credenciales necesarias para autenticarse contra Vertex AI.

    La prioridad de carga es la siguiente:
    1. Un diccionario ``json_credentials`` proporcionado explícitamente.
    2. Un ``credentials_path`` proporcionado explícitamente.
    3. El valor de la variable de entorno ``path_env_var``. Se acepta tanto la ruta
       a un archivo JSON como el propio contenido JSON en formato string.
    4. Un ``default_path`` relativo al proyecto.

    Si ninguna fuente es válida, se lanza ``FileNotFoundError``.
    """

    if json_credentials is not None:
        return _build_credentials_from_info(json_credentials)

    if credentials_path is not None:
        return _build_credentials_from_file(Path(credentials_path).expanduser())

    env_value = os.getenv(path_env_var)
    if env_value:
        env_value = env_value.strip()
        if env_value.startswith("{"):
            try:
                info = json.loads(env_value)
            except json.JSONDecodeError as exc:
                LOGGER.error(
                    "La variable de entorno %s no contiene un JSON válido de credenciales.",
                    path_env_var,
                )
                raise ValueError("JSON de credenciales inválido en variable de entorno") from exc
            return _build_credentials_from_info(info)
        return _build_credentials_from_file(Path(env_value).expanduser())

    if default_path is not None:
        default_path = Path(default_path).expanduser()
        if default_path.exists():
            return _build_credentials_from_file(default_path)

    message = (
        "No se encontraron credenciales de Vertex AI. Define la variable de entorno "
        f"{path_env_var} o coloca el archivo en {DEFAULT_CREDENTIALS_PATH}."
    )
    LOGGER.error(message)
    raise FileNotFoundError(message)


def init_gemini_llm(
    credentials: (
        service_account.Credentials
        | Mapping[str, Any]
        | MutableMapping[str, Any]
        | None
    ) = None,
    *,
    project_id: str | None = None,
    location: str | None = None,
    model_name: str | None = None,
    temperature: float = 0.2,
    max_output_tokens: int | None = None,
    top_p: float | None = None,
    top_k: int | None = None,
    request_timeout: float | None = None,
    **extra_vertex_params: Any,
) -> VertexAI:
    """Inicializa y devuelve una instancia ``VertexAI`` configurada para Gemini."""

    project = project_id or os.getenv("VERTEX_PROJECT_ID")
    if not project:
        raise ValueError(
            "project_id es requerido para inicializar Gemini. Establece VERTEX_PROJECT_ID"
            " o pásalo como argumento."
        )

    resolved_location = location or os.getenv("VERTEX_LOCATION", DEFAULT_VERTEX_LOCATION)
    resolved_model = model_name or os.getenv("VERTEX_MODEL", DEFAULT_GEMINI_MODEL)

    if credentials is None:
        credentials_obj = load_vertex_credentials()
    elif isinstance(credentials, service_account.Credentials):
        credentials_obj = credentials
    elif isinstance(credentials, Mapping):
        credentials_obj = _build_credentials_from_info(credentials)
    else:
        raise TypeError(
            "El parámetro credentials debe ser un objeto Credentials o un diccionario con el JSON del service account."
        )

    if getattr(credentials_obj, "requires_scopes", False):  # pragma: no cover - depende de versión
        credentials_obj = credentials_obj.with_scopes(VERTEX_SCOPES)

    client_kwargs: Dict[str, Any] = {
        "model": resolved_model,
        "temperature": temperature,
        "project": project,
        "location": resolved_location,
        "credentials": credentials_obj,
    }

    if max_output_tokens is not None:
        client_kwargs["max_output_tokens"] = max_output_tokens
    if top_p is not None:
        client_kwargs["top_p"] = top_p
    if top_k is not None:
        client_kwargs["top_k"] = top_k
    if request_timeout is not None:
        client_kwargs["request_timeout"] = request_timeout
    if extra_vertex_params:
        client_kwargs.update(extra_vertex_params)

    try:
        llm = VertexAI(**client_kwargs)
    except Exception as exc:  # pragma: no cover - depende del entorno de Vertex AI
        LOGGER.exception("Error al inicializar el modelo Gemini en Vertex AI")
        raise RuntimeError("No se pudo inicializar el modelo Gemini de Vertex AI") from exc

    return llm


__all__ = ["load_vertex_credentials", "init_gemini_llm"]
