"""Wrapper around the BigQuery API to execute read-only queries."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

from google.cloud import bigquery
from google.oauth2 import service_account

ALLOWED_PREFIX = "select"
BLOCKED_KEYWORDS = {"delete", "update", "drop", "truncate", "alter", "insert"}
MAX_ROWS = 1000


class BigQueryClient:
    """Minimal BigQuery client tailored for the CrewAI executor agent."""

    def __init__(
        self,
        credentials_path: str | Path = "config/bq_service_account.json",
        default_project: Optional[str] = None,
        max_rows: int = MAX_ROWS,
    ) -> None:
        self.credentials_info = self._load_credentials(credentials_path)
        self.credentials = service_account.Credentials.from_service_account_info(
            self.credentials_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.project_id = (
            default_project
            or self.credentials_info.get("project_id")
            or os.environ.get("BIGQUERY_PROJECT_ID")
        )
        if not self.project_id:
            raise ValueError(
                "No se pudo determinar el ID de proyecto para BigQuery."
            )
        self.client = bigquery.Client(project=self.project_id, credentials=self.credentials)
        self.max_rows = max_rows

    @staticmethod
    def _load_credentials(path: str | Path) -> Dict[str, str]:
        path_obj = Path(path)
        if not path_obj.exists():
            raise FileNotFoundError(
                f"No se encontró el archivo de credenciales de BigQuery en {path_obj}."
            )
        with path_obj.open("r", encoding="utf-8") as fh:
            return json.load(fh)

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


__all__ = ["BigQueryClient"]
