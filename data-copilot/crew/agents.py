"""CrewAI agent definitions and helper tools."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from crewai import Agent
from crewai.tools import BaseTool
from pydantic import Field

import sqlparse
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.tokens import Keyword


class ConversationHistoryTool(BaseTool):
    """Expose the chat history as a CrewAI tool."""

    name: str = "conversation_history"
    description: str = (
        "Proporciona el historial completo de la conversación para ayudar a "
        "interpretar la nueva solicitud del usuario."
    )
    history: str = Field(
        default="",
        description="Historial completo de mensajes previos en la conversación.",
    )

    def set_history(self, history: str) -> None:
        """Update the cached conversation history."""

        self.history = history

    def _run(self) -> str:
        return self.history or "(La conversación inicia con este mensaje)"


class SQLMetadataTool(BaseTool):
    """Expose table metadata stored in JSON files as a CrewAI tool."""

    name: str = "sql_metadata_lookup"
    description: str = (
        "Devuelve metadatos del modelo relacional para ayudar a generar SQL. "
        "Permite consultar descripciones de tablas, columnas y relaciones."
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Metadatos disponibles del modelo relacional.",
    )

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

    def _run(self, table: str | None = None) -> str:
        if not self.metadata:
            return "{}"
        if table and table in self.metadata:
            return json.dumps(self.metadata[table], ensure_ascii=False, indent=2)
        return json.dumps(self.metadata, ensure_ascii=False, indent=2)


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


class SQLValidationTool(BaseTool):
    """Applies deterministic validation rules to generated SQL statements."""

    name: str = "sql_validation_tool"
    description: str = (
        "Valida una consulta SQL para asegurar que solo se realicen operaciones de lectura, "
        "que se respeten los límites y que las tablas y columnas existan en el modelo."
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)
    max_limit: int = Field(default=1000)
    audit_path: Path = Field(
        default=Path(__file__).resolve().parent.parent
        / "data"
        / "logs"
        / "sql_audit.json"
    )
    candidate_sql: str = Field(default="")
    question: str = Field(default="")

    _blocked_keywords = {
        "delete",
        "update",
        "drop",
        "alter",
        "insert",
        "truncate",
        "merge",
    }

    def set_metadata(self, metadata: Dict[str, Any]) -> None:
        self.metadata = metadata or {}

    def set_candidate(self, sql: str, question: str | None = None) -> None:
        self.candidate_sql = sql or ""
        self.question = question or ""

    def _run(self, sql: str | None = None) -> str:
        statement = sql or self.candidate_sql
        result = validate_sql_statement(
            statement,
            metadata=self.metadata,
            max_limit=self.max_limit,
            audit_path=self.audit_path,
            blocked_keywords=self._blocked_keywords,
            question=self.question,
        )
        return json.dumps(result, ensure_ascii=False)


class GeminiAnalysisTool(BaseTool):
    """Bridges the Analyzer agent with the Gemini client."""

    name: str = "gemini_result_analyzer"
    description: str = (
        "Genera un análisis narrativo en español a partir de los resultados devueltos por BigQuery."
    )
    client: "GeminiClient"
    question: str = Field(default="")
    sql: str = Field(default="")
    results: list[dict[str, Any]] = Field(default_factory=list)

    def set_context(
        self,
        *,
        question: str | None = None,
        sql: str | None = None,
        results: list[dict[str, Any]] | None = None,
    ) -> None:
        self.question = question or ""
        self.sql = sql or ""
        self.results = results or []

    def _run(self, _: str | None = None) -> str:
        analysis = self.client.analyze_results(
            self.results,
            question=self.question or None,
            sql=self.sql or None,
        )
        return json.dumps(analysis, ensure_ascii=False)


def validate_sql_statement(
    sql: str,
    *,
    metadata: Dict[str, Any],
    max_limit: int,
    audit_path: Path,
    blocked_keywords: set[str],
    question: str | None = None,
) -> Dict[str, Any]:
    """Validate SQL string against a set of deterministic security rules."""

    metadata = metadata or {}
    issues: list[str] = []
    warnings: list[str] = []
    sanitized_sql = (sql or "").strip()
    normalized = sanitized_sql.lower()

    if not sanitized_sql:
        issues.append("La sentencia SQL está vacía.")

    if sanitized_sql.endswith(";"):
        sanitized_sql = sanitized_sql.rstrip(";\n\t \r")
        warnings.append("Se eliminó el punto y coma final de la sentencia.")
        normalized = sanitized_sql.lower()

    if normalized and not normalized.startswith("select"):
        issues.append("Solo se permiten consultas SELECT.")

    for keyword in blocked_keywords:
        pattern = rf"\b{re.escape(keyword)}\b"
        if re.search(pattern, normalized):
            issues.append("La consulta contiene palabras clave no permitidas.")
            break

    parsed = sqlparse.parse(sanitized_sql or "")
    if not parsed:
        issues.append("No se pudo analizar la consulta SQL.")
        statement = None
    else:
        statement = parsed[0]
        if statement.get_type() != "SELECT":
            issues.append("Solo se permiten consultas SELECT.")

    catalog = build_metadata_catalog(metadata)
    alias_map: Dict[str, str] = {}
    referenced_tables: list[str] = []
    if statement is not None:
        referenced_tables = extract_tables(statement)
        for table in referenced_tables:
            entry = resolve_table(table, catalog)
            if entry is None:
                issues.append(f"La tabla '{table}' no está autorizada por el modelo.")
            else:
                alias = entry.get("alias")
                canonical = entry["name"]
                if alias:
                    alias_map[alias] = canonical
                alias_map[canonical] = canonical
                path_key = entry.get("path")
                if isinstance(path_key, str):
                    alias_map[path_key] = canonical

        column_issues = validate_columns(sanitized_sql, alias_map, catalog)
        issues.extend(column_issues)

    has_limit = bool(re.search(r"\blimit\b", normalized))
    enforced_limit = False
    if not has_limit and not issues:
        sanitized_sql = f"{sanitized_sql} LIMIT {max_limit}"
        enforced_limit = True
        warnings.append(f"Se aplicó automáticamente LIMIT {max_limit}.")

    message = "Consulta validada correctamente." if not issues else "La consulta fue rechazada por el validador."
    result = {
        "valid": not issues,
        "sanitized_sql": sanitized_sql if not issues else None,
        "issues": issues,
        "warnings": warnings,
        "message": message,
        "question": question,
        "tables": referenced_tables,
        "enforced_limit": enforced_limit,
    }

    log_sql_audit(
        audit_path,
        {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "question": question,
            "submitted_sql": sql,
            "sanitized_sql": result.get("sanitized_sql"),
            "valid": result["valid"],
            "issues": issues,
            "warnings": warnings,
        },
    )

    return result


def build_metadata_catalog(metadata: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Create a lookup dictionary for tables and their columns."""

    catalog: Dict[str, Dict[str, Any]] = {}
    for table_key, table_data in metadata.items():
        if isinstance(table_data, dict) and table_key in table_data:
            info = table_data.get(table_key, {})
        else:
            info = table_data
        if not isinstance(info, dict):
            continue
        columns = info.get("columns") or info.get("columnas")
        if isinstance(columns, dict):
            column_names = {col.lower() for col in columns.keys()}
        else:
            column_names = set()
        path = info.get("path") or info.get("tabla")
        canonical = table_key.lower()
        entry = {
            "name": canonical,
            "path": str(path).lower() if isinstance(path, str) else None,
            "columns": column_names,
        }
        catalog[canonical] = entry
        if entry["path"]:
            catalog[entry["path"]] = entry
            dataset_table = entry["path"].split(".")[-1]
            catalog[dataset_table] = entry
    return catalog


def extract_tables(statement: sqlparse.sql.Statement) -> list[str]:
    """Extract table identifiers from a SQL statement."""

    tables: list[str] = []
    for token in statement.tokens:
        if token.is_group:
            tables.extend(extract_tables(token))
        if token.ttype is Keyword and token.value.upper() in {"FROM", "JOIN"}:
            _, next_token = statement.token_next(statement.token_index(token))
            if isinstance(next_token, IdentifierList):
                for identifier in next_token.get_identifiers():
                    tables.append(identifier.value)
            elif isinstance(next_token, Identifier):
                tables.append(next_token.value)
    return tables


def resolve_table(raw_identifier: str, catalog: Dict[str, Dict[str, Any]]) -> Dict[str, Any] | None:
    """Resolve a raw table identifier against the metadata catalog."""

    cleaned = raw_identifier.replace("`", "").strip()
    alias = None
    parts = re.split(r"\s+", cleaned, maxsplit=2)
    table_name = parts[0]
    if len(parts) >= 3 and parts[1].lower() == "as":
        alias = parts[2].split()[0].lower()
    elif len(parts) >= 2:
        alias = parts[1].lower()
    normalized = table_name.lower()
    entry = catalog.get(normalized)
    if entry is None and "." in normalized:
        entry = catalog.get(normalized.split(".")[-1])
    if entry is None:
        return None
    resolved = dict(entry)
    resolved["alias"] = alias
    return resolved


def validate_columns(
    sql: str,
    alias_map: Dict[str, str],
    catalog: Dict[str, Dict[str, Any]],
) -> list[str]:
    """Check that referenced columns belong to authorised tables."""

    issues: list[str] = []
    pattern = re.compile(r"\b([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\b")
    for alias, column in pattern.findall(sql):
        alias_norm = alias.lower()
        column_norm = column.lower()
        lookup_key = alias_map.get(alias_norm, alias_norm)
        entry = catalog.get(lookup_key)
        if not entry:
            continue
        columns = entry.get("columns") or set()
        if columns and column_norm not in columns:
            issues.append(
                f"La columna '{column}' no está permitida en la tabla '{entry['name']}'."
            )
    return issues


def log_sql_audit(audit_path: Path, entry: Dict[str, Any]) -> None:
    """Append validation attempts to the audit log."""

    audit_path.parent.mkdir(parents=True, exist_ok=True)
    existing: list[Dict[str, Any]] = []
    if audit_path.exists():
        try:
            with audit_path.open("r", encoding="utf-8") as handle:
                existing_data = json.load(handle)
                if isinstance(existing_data, list):
                    existing = existing_data
        except json.JSONDecodeError:
            existing = []
    existing.append(entry)
    with audit_path.open("w", encoding="utf-8") as handle:
        json.dump(existing, handle, indent=2, ensure_ascii=False)


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
        verbose=True,
        tools=[query_tool],
        llm=llm,
    )


def create_validator_agent(
    validation_tool: SQLValidationTool,
    llm: Any | None = None,
) -> Agent:
    """Create the agent in charge of vetting SQL statements."""

    return Agent(
        role="ValidatorAgent",
        goal=(
            "Revisar que la consulta SQL generada sea segura, respete las políticas "
            "de solo lectura y se limite a las tablas y columnas autorizadas."
        ),
        backstory=(
            "Eres un especialista en gobernanza de datos. Debes utilizar el tool "
            "de validación para aprobar o rechazar las consultas antes de que se "
            "ejecuten."
        ),
        allow_delegation=False,
        verbose=True,
        tools=[validation_tool],
        llm=llm,
    )


def create_analyzer_agent(
    analysis_tool: GeminiAnalysisTool,
    llm: Any | None = None,
) -> Agent:
    """Create the agent responsible for crafting the narrative answer."""

    return Agent(
        role="AnalyzerAgent",
        goal=(
            "Interpretar los resultados numéricos provenientes de BigQuery y "
            "comunicar hallazgos en español en un lenguaje ejecutivo."
        ),
        backstory=(
            "Eres un analista de inteligencia de negocio que sintetiza datos en "
            "historias claras para la dirección. Utiliza el tool de análisis para "
            "generar texto y sugerencias visuales."
        ),
        allow_delegation=False,
        verbose=True,
        tools=[analysis_tool],
        llm=llm,
    )


__all__ = [
    "ConversationHistoryTool",
    "SQLMetadataTool",
    "BigQueryQueryTool",
    "SQLValidationTool",
    "GeminiAnalysisTool",
    "create_interpreter_agent",
    "create_sql_generator_agent",
    "create_executor_agent",
    "create_validator_agent",
    "create_analyzer_agent",
    "load_model_metadata",
    "validate_sql_statement",
]


# ``BigQueryQueryTool`` usa una referencia adelantada a ``BigQueryClient``.
# Importarlo al final del módulo y reconstruir el modelo le indica a Pydantic
# cómo resolver esa anotación y evita el error "class not fully defined".
from services.bigquery_client import BigQueryClient  # noqa: E402  (import tardío)
from services.gemini_client import GeminiClient  # noqa: E402  (import tardío)

BigQueryQueryTool.model_rebuild()
GeminiAnalysisTool.model_rebuild()
