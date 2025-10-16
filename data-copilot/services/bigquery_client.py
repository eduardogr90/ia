"""Wrapper around the BigQuery API to execute read-only queries."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from google.cloud import bigquery
from google.oauth2 import service_account

LOGGER = logging.getLogger(__name__)

ALLOWED_PREFIX = "select"
BLOCKED_KEYWORDS = {"delete", "update", "drop", "truncate", "alter", "insert"}
MAX_ROWS = 1000
DEFAULT_BIGQUERY_CREDENTIALS_PATH = (
    Path(__file__).resolve().parent.parent / "config" / "bq_service_account.json"
)
BIGQUERY_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)


def _read_json_file(path: Path) -> Mapping[str, Any]:
    """Lee un archivo JSON y devuelve su contenido como diccionario."""

    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError as exc:
        LOGGER.error("No se encontró el archivo de credenciales de BigQuery: %s", path)
        raise
    except json.JSONDecodeError as exc:
        LOGGER.error("El archivo de credenciales de BigQuery no es un JSON válido: %s", path)
        raise ValueError("Credenciales de BigQuery inválidas: JSON corrupto") from exc


def _validate_credentials_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Valida que el diccionario tenga los campos mínimos requeridos."""

    required_keys = {"type", "project_id", "private_key", "client_email"}
    missing = [key for key in required_keys if key not in payload or not payload[key]]
    if missing:
        raise ValueError(
            "Las credenciales de BigQuery son inválidas. Faltan las claves: "
            + ", ".join(missing)
        )
    return dict(payload)


def load_bigquery_credentials(
    *,
    credentials_path: str | os.PathLike[str] | None = None,
    json_credentials: Mapping[str, Any] | MutableMapping[str, Any] | None = None,
    json_env_var: str = "BIGQUERY_CREDENTIALS_JSON",
    path_env_var: str = "BIGQUERY_CREDENTIALS_PATH",
    default_path: str | os.PathLike[str] | None = DEFAULT_BIGQUERY_CREDENTIALS_PATH,
) -> Dict[str, Any]:
    """Carga y valida credenciales de BigQuery.

    Las credenciales pueden obtenerse de varias fuentes (se evalúan en este orden):

    1. El parámetro ``json_credentials`` con el contenido ya cargado.
    2. El parámetro ``credentials_path`` apuntando a un archivo JSON.
    3. La variable de entorno ``json_env_var`` con el JSON como texto.
    4. La variable de entorno ``path_env_var`` con la ruta al archivo.
    5. ``default_path`` relativo al repositorio ``data-copilot``.

    El JSON debe seguir el formato estándar de cuentas de servicio de Google Cloud,
    por ejemplo::

        {
            "type": "service_account",
            "project_id": "mi-proyecto",
            "private_key_id": "...",
            "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
            "client_email": "mi-servicio@mi-proyecto.iam.gserviceaccount.com",
            "client_id": "...",
            "token_uri": "https://oauth2.googleapis.com/token"
        }

    Returns:
        Diccionario con el contenido de las credenciales listo para crear un
        ``Credentials`` de ``google.oauth2.service_account``.
    """

    if json_credentials is not None:
        return _validate_credentials_payload(json_credentials)

    if credentials_path is not None:
        return _validate_credentials_payload(
            _read_json_file(Path(credentials_path).expanduser())
        )

    json_env_value = os.getenv(json_env_var)
    if json_env_value:
        try:
            payload = json.loads(json_env_value)
        except json.JSONDecodeError as exc:
            LOGGER.error(
                "La variable de entorno %s no contiene JSON válido de credenciales.",
                json_env_var,
            )
            raise ValueError("JSON inválido en variables de entorno de BigQuery") from exc
        return _validate_credentials_payload(payload)

    path_env_value = os.getenv(path_env_var)
    if path_env_value:
        return _validate_credentials_payload(
            _read_json_file(Path(path_env_value).expanduser())
        )

    if default_path is not None:
        default_path = Path(default_path).expanduser()
        if default_path.exists():
            return _validate_credentials_payload(_read_json_file(default_path))

    message = (
        "No se encontraron credenciales de BigQuery."
        " Define BIGQUERY_CREDENTIALS_JSON, BIGQUERY_CREDENTIALS_PATH"
        " o coloca el archivo en config/bq_service_account.json."
    )
    LOGGER.error(message)
    raise FileNotFoundError(message)


class BigQueryClient:
    """Minimal BigQuery client tailored for the CrewAI executor agent."""

    def __init__(
        self,
        *,
        credentials_path: str | Path | None = None,
        credentials_info: Mapping[str, Any] | MutableMapping[str, Any] | None = None,
        default_project: Optional[str] = None,
        max_rows: int = MAX_ROWS,
    ) -> None:
        self.credentials_info = load_bigquery_credentials(
            credentials_path=credentials_path,
            json_credentials=credentials_info,
        )
        self.credentials = service_account.Credentials.from_service_account_info(
            self.credentials_info,
            scopes=BIGQUERY_SCOPES,
        )
        self.project_id = (
            default_project
            or self.credentials_info.get("project_id")
            or os.environ.get("BIGQUERY_PROJECT_ID")
        )
        if not self.project_id:
            raise ValueError("No se pudo determinar el ID de proyecto para BigQuery.")
        self.client = bigquery.Client(project=self.project_id, credentials=self.credentials)
        self.max_rows = max_rows

    # ------------------------------------------------------------------
    def _validate_sql(self, sql: str) -> str:
        statement = sql.strip().rstrip(";")
        collapsed = " ".join(statement.lower().split())
        if not collapsed.startswith(ALLOWED_PREFIX):
            raise ValueError("Solo se permiten consultas SELECT en BigQuery.")
        if any(keyword in collapsed for keyword in BLOCKED_KEYWORDS):
            raise ValueError("La consulta contiene palabras clave no permitidas.")
        if "--" in statement or ";" in statement:
            raise ValueError("No se permiten comentarios ni múltiples sentencias.")
        if " limit " not in collapsed and not collapsed.endswith(" limit"):
            statement = f"{statement} LIMIT {self.max_rows}"
        return statement

    def run_query(self, sql: str) -> List[Dict[str, object]]:
        """Execute a read-only query and return the rows as dictionaries."""

        statement = self._validate_sql(sql)
        try:
            job = self.client.query(statement)
            rows = job.result(max_results=self.max_rows)
        except Exception as exc:  # pragma: no cover - requires BigQuery connection
            raise RuntimeError(f"Error al ejecutar la consulta en BigQuery: {exc}")
        return [dict(row.items()) for row in rows]


__all__ = ["BigQueryClient", "load_bigquery_credentials"]
