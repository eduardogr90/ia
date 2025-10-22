"""Executor agent responsible for running SQL statements."""
from __future__ import annotations

import json
from typing import Any, Optional

from crewai import Agent
from crewai.tools import BaseTool
from pydantic import Field


class BigQueryQueryTool(BaseTool):
    """Tool wrapper that proxies execution to the ``BigQueryClient``."""

    name: str = "bigquery_sql_runner"
    description: str = (
        "Ejecuta consultas SELECT en BigQuery y devuelve los resultados en formato JSON. "
        "Utiliza este tool con la cadena SQL completa como parámetro."
    )
    client: "BigQueryClient"
    last_result: Optional[list[dict[str, Any]]] = Field(
        default=None, description="Últimos resultados devueltos por BigQuery."
    )
    last_error: Optional[str] = Field(
        default=None, description="Último error registrado al ejecutar SQL."
    )
    last_sql: Optional[str] = Field(
        default=None, description="Última sentencia SQL ejecutada."
    )

    def reset(self) -> None:
        """Reset cached results between runs."""

        self.last_result = None
        self.last_error = None
        self.last_sql = None

    def _run(self, sql: str) -> str:
        self.last_sql = sql
        try:
            rows = self.client.run_query(sql)
        except Exception as exc:  # pragma: no cover - runtime errors
            self.last_result = None
            self.last_error = str(exc)
            return json.dumps({"error": self.last_error}, ensure_ascii=False)
        self.last_result = rows
        self.last_error = None
        return json.dumps(
            {
                "row_count": len(rows),
                "rows": rows,
            },
            ensure_ascii=False,
        )


def create_executor_agent(
    query_tool: BigQueryQueryTool,
    llm: Any | None = None,
) -> Agent:
    """Create the agent responsible for running SQL statements."""

    return Agent(
        role="ExecutorAgent",
        goal=(
            "Ejecutar consultas en BigQuery de forma segura utilizando el tool "
            "proporcionado y devolver los datos sin interpretar los resultados."
        ),
        backstory=(
            "Eres un ingeniero de datos con acceso controlado a BigQuery. Tu "
            "responsabilidad es ejecutar consultas ya validadas, revisar el "
            "resultado de la ejecución y reportar errores técnicos si se "
            "presentan. El análisis narrativo será realizado por otro agente."
        ),
        allow_delegation=False,
        verbose=False,
        tools=[query_tool],
        llm=llm,
    )


__all__ = ["BigQueryQueryTool", "create_executor_agent"]

# ``BigQueryQueryTool`` usa una referencia adelantada a ``BigQueryClient``.
# Importarlo al final del módulo y reconstruir el modelo le indica a Pydantic
# cómo resolver esa anotación y evita el error "class not fully defined".
from services.bigquery_client import BigQueryClient  # noqa: E402  (import tardío)

BigQueryQueryTool.model_rebuild()
