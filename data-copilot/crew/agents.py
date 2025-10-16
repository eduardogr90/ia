"""CrewAI agent definitions and helper tools."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from crewai import Agent


class ConversationHistoryTool:
    """Expose the chat history as a CrewAI tool."""

    name = "conversation_history"
    description = (
        "Proporciona el historial completo de la conversación para ayudar a "
        "interpretar la nueva solicitud del usuario."
    )

    def __init__(self, history: str | None = None) -> None:
        self.history = history or ""

    def set_history(self, history: str) -> None:
        """Update the cached conversation history."""

        self.history = history

    def __call__(self) -> str:
        return self.history or "(La conversación inicia con este mensaje)"


class SQLMetadataTool:
    """Expose table metadata stored in JSON files as a CrewAI tool."""

    name = "sql_metadata_lookup"
    description = (
        "Devuelve metadatos del modelo relacional para ayudar a generar SQL. "
        "Permite consultar descripciones de tablas, columnas y relaciones."
    )

    def __init__(self, metadata: Optional[Dict[str, Any]] = None) -> None:
        self.metadata: Dict[str, Any] = metadata or {}

    def set_metadata(self, metadata: Dict[str, Any]) -> None:
        """Replace the metadata dictionary."""

        self.metadata = metadata or {}

    def summary(self) -> str:
        """Return a human readable summary of the available metadata."""

        if not self.metadata:
            return "No hay metadatos disponibles."
        sections = []
        for table, table_data in self.metadata.items():
            description = table_data.get("description", "")
            columns = table_data.get("columns", [])
            column_lines = []
            for column in columns:
                col_name = column.get("name")
                col_desc = column.get("description", "")
                if col_desc:
                    column_lines.append(f"- {col_name}: {col_desc}")
                else:
                    column_lines.append(f"- {col_name}")
            section = [f"Tabla: {table}"]
            if description:
                section.append(f"Descripción: {description}")
            if column_lines:
                section.append("Columnas:\n" + "\n".join(column_lines))
            sections.append("\n".join(section))
        return "\n\n".join(sections)

    def __call__(self, table: str | None = None) -> str:
        if not self.metadata:
            return "{}"
        if table and table in self.metadata:
            return json.dumps(self.metadata[table], ensure_ascii=False, indent=2)
        return json.dumps(self.metadata, ensure_ascii=False, indent=2)


class BigQueryQueryTool:
    """Tool wrapper that proxies execution to the ``BigQueryClient``."""

    name = "bigquery_sql_runner"
    description = (
        "Ejecuta consultas SELECT en BigQuery y devuelve los resultados en formato JSON. "
        "Utiliza este tool con la cadena SQL completa como parámetro."
    )

    def __init__(self, client: "BigQueryClient") -> None:  # pragma: no cover - only type hint
        self.client = client
        self.last_result: Optional[list[dict[str, Any]]] = None
        self.last_error: Optional[str] = None
        self.last_sql: Optional[str] = None

    def reset(self) -> None:
        """Reset cached results between runs."""

        self.last_result = None
        self.last_error = None
        self.last_sql = None

    def __call__(self, sql: str) -> str:
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


def load_model_metadata(metadata_dir: Path) -> Dict[str, Any]:
    """Load every ``*.json`` file from ``data/model`` into memory."""

    metadata: Dict[str, Any] = {}
    if not metadata_dir.exists():
        return metadata
    for path in sorted(metadata_dir.glob("*.json")):
        try:
            with path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except json.JSONDecodeError:
            continue
        table_name = data.get("table") or path.stem
        metadata[table_name] = data
    return metadata


def create_interpreter_agent(
    history_tool: ConversationHistoryTool,
    llm: Any | None = None,
) -> Agent:
    """Create the agent responsible for understanding the user's intent."""

    return Agent(
        role="InterpreterAgent",
        goal=(
            "Analizar la intención del usuario, comprender el contexto de la "
            "conversación y determinar si se requiere una consulta SQL."
        ),
        backstory=(
            "Eres un analista de datos senior con una gran capacidad para "
            "interpretar preguntas en lenguaje natural y decidir si es necesario "
            "consultar la base de datos para responderlas."
        ),
        allow_delegation=False,
        verbose=True,
        tools=[history_tool],
        llm=llm,
    )


def create_sql_generator_agent(
    metadata_tool: SQLMetadataTool,
    llm: Any | None = None,
) -> Agent:
    """Create the agent that converts intents into SQL queries."""

    return Agent(
        role="SQLGeneratorAgent",
        goal=(
            "Transformar preguntas de negocio en consultas SQL válidas basadas en "
            "los metadatos del modelo y en las convenciones de BigQuery."
        ),
        backstory=(
            "Eres un experto en modelado de datos analíticos y puedes combinar "
            "diferentes tablas según las relaciones definidas en los metadatos."
        ),
        allow_delegation=False,
        verbose=True,
        tools=[metadata_tool],
        llm=llm,
    )


def create_executor_agent(
    query_tool: BigQueryQueryTool,
    llm: Any | None = None,
) -> Agent:
    """Create the agent responsible for running SQL statements."""

    return Agent(
        role="ExecutorAgent",
        goal=(
            "Ejecutar consultas en BigQuery de forma segura, analizar los resultados "
            "y construir una respuesta clara para el usuario final."
        ),
        backstory=(
            "Eres un ingeniero de datos con acceso a BigQuery. Sabes revisar la "
            "seguridad de las consultas antes de ejecutarlas y explicar los "
            "hallazgos con claridad."
        ),
        allow_delegation=False,
        verbose=True,
        tools=[query_tool],
        llm=llm,
    )


__all__ = [
    "ConversationHistoryTool",
    "SQLMetadataTool",
    "BigQueryQueryTool",
    "create_interpreter_agent",
    "create_sql_generator_agent",
    "create_executor_agent",
    "load_model_metadata",
]
