"""Cliente utilitario para inicializar modelos Gemini de Vertex AI."""
from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping

from google.oauth2 import service_account
from langchain_google_vertexai import VertexAI

LOGGER = logging.getLogger(__name__)

DEFAULT_VERTEX_LOCATION = "us-central1"
DEFAULT_GEMINI_MODEL = "gemini-2.0-flash-lite-001"
DEFAULT_CREDENTIALS_PATH = (
    Path(__file__).resolve().parent.parent / "config" / "json_key_vertex.json"
)
VERTEX_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)


def _tag_credentials(
    credentials: service_account.Credentials,
    info: Mapping[str, Any] | None,
) -> service_account.Credentials:
    """Adjunta metadatos útiles al objeto de credenciales."""

    if info is None:
        return credentials

    raw_copy = dict(info)
    setattr(credentials, "_ia_raw_info", raw_copy)
    project_id = raw_copy.get("project_id")
    if project_id:
        setattr(credentials, "_ia_project_id", project_id)
    return credentials


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
    return _tag_credentials(credentials, info)


def _load_credentials_info_from_file(path: Path) -> Mapping[str, Any]:
    """Lee un archivo JSON y devuelve su contenido como diccionario."""

    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        LOGGER.error("No se encontró el archivo de credenciales de Vertex AI: %s", path)
        raise
    except json.JSONDecodeError as exc:
        LOGGER.error(
            "El archivo de credenciales de Vertex AI no contiene un JSON válido: %s",
            path,
        )
        raise ValueError("Credenciales de Vertex AI inválidas") from exc
    except OSError as exc:
        LOGGER.error("No se pudo leer el archivo de credenciales de Vertex AI: %s", path)
        raise RuntimeError("No fue posible leer el archivo de credenciales de Vertex AI") from exc


def _build_credentials_from_file(path: Path) -> service_account.Credentials:
    """Carga credenciales de servicio desde un archivo JSON."""

    info = _load_credentials_info_from_file(path)
    return _build_credentials_from_info(info)


def _ensure_adc_environment(
    info: Mapping[str, Any],
    *,
    path_env_var: str,
    existing_path: Path | None,
) -> Path:
    """Garantiza que exista un archivo utilizable como Application Default Credentials."""

    adc_path = existing_path
    payload = dict(info)
    if adc_path is None:
        temp_dir = Path(tempfile.gettempdir())
        adc_path = temp_dir / "vertex_application_default_credentials.json"
        with adc_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle)

    path_str = str(adc_path)
    os.environ.setdefault(path_env_var, path_str)
    if path_env_var != "GOOGLE_APPLICATION_CREDENTIALS":
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", path_str)
    return adc_path


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

    credentials_info: Mapping[str, Any] | None = None
    source_path: Path | None = None

    if json_credentials is not None:
        credentials_info = dict(json_credentials)
    elif credentials_path is not None:
        source_path = Path(credentials_path).expanduser()
        credentials_info = _load_credentials_info_from_file(source_path)
    else:
        env_value = os.getenv(path_env_var)
        if env_value:
            env_value = env_value.strip()
            if env_value.startswith("{"):
                try:
                    credentials_info = json.loads(env_value)
                except json.JSONDecodeError as exc:
                    LOGGER.error(
                        "La variable de entorno %s no contiene un JSON válido de credenciales.",
                        path_env_var,
                    )
                    raise ValueError("JSON de credenciales inválido en variable de entorno") from exc
            else:
                source_path = Path(env_value).expanduser()
                credentials_info = _load_credentials_info_from_file(source_path)
        if credentials_info is None and default_path is not None:
            default_path = Path(default_path).expanduser()
            if default_path.exists():
                source_path = default_path
                credentials_info = _load_credentials_info_from_file(default_path)

    if credentials_info is None:
        message = (
            "No se encontraron credenciales de Vertex AI. Define la variable de entorno "
            f"{path_env_var} o coloca el archivo en {DEFAULT_CREDENTIALS_PATH}."
        )
        LOGGER.error(message)
        raise FileNotFoundError(message)

    _ensure_adc_environment(credentials_info, path_env_var=path_env_var, existing_path=source_path)

    return _build_credentials_from_info(credentials_info)


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

    resolved_location = location or os.getenv("VERTEX_LOCATION", DEFAULT_VERTEX_LOCATION)
    resolved_model = model_name or os.getenv("VERTEX_MODEL", DEFAULT_GEMINI_MODEL)

    credentials_info: Mapping[str, Any] | None = None
    if credentials is None:
        credentials_obj = load_vertex_credentials()
    elif isinstance(credentials, service_account.Credentials):
        credentials_obj = credentials
        credentials_info = getattr(credentials_obj, "_ia_raw_info", None)
    elif isinstance(credentials, Mapping):
        credentials_info = credentials
        credentials_obj = _build_credentials_from_info(credentials)
    else:
        raise TypeError(
            "El parámetro credentials debe ser un objeto Credentials o un diccionario con el JSON del service account."
        )

    if getattr(credentials_obj, "requires_scopes", False):  # pragma: no cover - depende de versión
        credentials_obj = credentials_obj.with_scopes(VERTEX_SCOPES)

    project = (
        project_id
        or os.getenv("VERTEX_PROJECT_ID")
        or (credentials_info or {}).get("project_id")
        or getattr(credentials_obj, "project_id", None)
        or getattr(credentials_obj, "_ia_project_id", None)
        or getattr(credentials_obj, "_project_id", None)
    )

    if not project:
        raise ValueError(
            "No se pudo determinar el ID de proyecto de Vertex AI. Define VERTEX_PROJECT_ID "
            "o incluye project_id en el JSON de credenciales."
        )

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
        LOGGER.exception(
            "Error al inicializar el modelo Gemini en Vertex AI: %s",
            exc,
        )
        raise RuntimeError(
            f"No se pudo inicializar el modelo Gemini de Vertex AI: {exc}"
        ) from exc

    return llm


__all__ = [
    "DEFAULT_VERTEX_LOCATION",
    "load_vertex_credentials",
    "init_gemini_llm",
]
