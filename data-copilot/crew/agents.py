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

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError


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

    parsed_expression: exp.Expression | None = None
    if sanitized_sql:
        try:
            parsed_expression = sqlglot.parse_one(sanitized_sql or "", read="bigquery")
        except ParseError as exc:
            issues.append(f"No se pudo analizar la consulta SQL: {exc}.")
        except Exception:
            issues.append("No se pudo analizar la consulta SQL.")

    catalog = build_metadata_catalog(metadata)
    alias_map: Dict[str, str] = {}
    referenced_tables: list[str] = []

    if parsed_expression is not None:
        if not is_select_statement(parsed_expression):
            issues.append("Solo se permiten consultas SELECT.")

        table_results = analyze_tables(parsed_expression, catalog)
        referenced_tables = table_results["tables"]
        alias_map = table_results["aliases"]
        issues.extend(table_results["issues"])

        column_issues = collect_column_issues(parsed_expression, alias_map, catalog)
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


def normalize_identifier(identifier: str | None) -> str:
    """Normalize identifiers by removing BigQuery quotes and lowering the case."""

    if not identifier:
        return ""
    return identifier.replace("`", "").strip().lower()


def expression_name(value: Any) -> str:
    """Extract the textual representation of a sqlglot expression."""

    if value is None:
        return ""
    if isinstance(value, exp.Expression):
        if hasattr(value, "name"):
            name = value.name  # type: ignore[attr-defined]
            if isinstance(name, str):
                return name
        try:
            return value.sql(dialect="bigquery")
        except Exception:  # pragma: no cover - defensive
            return str(value)
    if isinstance(value, str):
        return value
    return str(value)


def extract_table_alias(table_expr: exp.Table) -> str | None:
    """Return the alias assigned to a table expression, if any."""

    alias_expr = table_expr.args.get("alias")
    if alias_expr is None:
        return None
    if isinstance(alias_expr, exp.TableAlias):
        if isinstance(alias_expr.this, exp.Identifier):
            return normalize_identifier(alias_expr.this.name)
        alias_name = expression_name(alias_expr.this)
        if alias_name:
            return normalize_identifier(alias_name)
    if isinstance(alias_expr, exp.Identifier):
        return normalize_identifier(alias_expr.name)
    alias_name = expression_name(alias_expr)
    if alias_name:
        return normalize_identifier(alias_name)
    return None


def analyze_tables(
    expression: exp.Expression, catalog: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """Validate table references and collect alias mappings."""

    issues: list[str] = []
    aliases: Dict[str, str] = {}
    referenced_tables: list[str] = []
    seen_tables: set[str] = set()

    for table_expr in expression.find_all(exp.Table):
        base_name = expression_name(table_expr.this)
        catalog_name = expression_name(table_expr.args.get("catalog"))
        db_name = expression_name(table_expr.args.get("db"))

        candidates: list[str] = []
        normalized_base = normalize_identifier(base_name)
        normalized_db = normalize_identifier(db_name)
        normalized_catalog = normalize_identifier(catalog_name)

        if normalized_base:
            candidates.append(normalized_base)
        if normalized_db and normalized_base:
            candidates.append(f"{normalized_db}.{normalized_base}")
        if normalized_catalog and normalized_db and normalized_base:
            candidates.append(f"{normalized_catalog}.{normalized_db}.{normalized_base}")
        if normalized_catalog and normalized_base:
            candidates.append(f"{normalized_catalog}.{normalized_base}")

        entry = None
        for candidate in candidates:
            entry = catalog.get(candidate)
            if entry:
                break

        display_name = base_name.replace("`", "") if base_name else base_name
        if entry is None:
            issues.append(
                f"La tabla '{display_name or table_expr.sql(dialect='bigquery')}' no está autorizada por el modelo."
            )
            continue

        canonical = entry["name"]
        if canonical not in seen_tables:
            referenced_tables.append(canonical)
            seen_tables.add(canonical)

        for alias_key in entry.get("aliases", {canonical}):
            aliases[alias_key] = canonical

        table_alias = extract_table_alias(table_expr)
        if table_alias:
            aliases[table_alias] = canonical

    return {
        "tables": referenced_tables,
        "aliases": aliases,
        "issues": issues,
    }


def collect_column_issues(
    expression: exp.Expression, alias_map: Dict[str, str], catalog: Dict[str, Dict[str, Any]]
) -> list[str]:
    """Check that referenced columns belong to authorised tables."""

    issues: list[str] = []
    for column_expr in expression.find_all(exp.Column):
        column_name = normalize_identifier(column_expr.name)
        if not column_name or column_name == "*":
            continue
        table_reference = normalize_identifier(column_expr.table)
        if not table_reference:
            continue
        canonical_table = alias_map.get(table_reference, table_reference)
        entry = catalog.get(canonical_table)
        if not entry:
            continue
        allowed_columns = entry.get("columns") or set()
        if allowed_columns and column_name not in allowed_columns:
            issues.append(
                f"La columna '{column_expr.name}' no está permitida en la tabla '{entry['name']}'."
            )
    return issues


def is_select_statement(expression: exp.Expression) -> bool:
    """Determine whether the parsed expression represents a read-only SELECT."""

    if isinstance(expression, exp.Select):
        return True
    if isinstance(expression, exp.With):
        return is_select_statement(expression.this)
    if isinstance(expression, exp.Subquery):
        return is_select_statement(expression.this)
    if isinstance(expression, exp.Limit):
        return is_select_statement(expression.this)
    if isinstance(expression, exp.Union):
        return True
    return expression.find(exp.Select) is not None


def build_metadata_catalog(metadata: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Create a lookup dictionary for tables and their columns."""

    catalog: Dict[str, Dict[str, Any]] = {}
    for table_key, table_data in metadata.items():
        info = table_data.get(table_key) if isinstance(table_data, dict) else table_data
        if not isinstance(info, dict):
            continue

        columns = info.get("columns") or info.get("columnas")
        if isinstance(columns, dict):
            column_names = {normalize_identifier(col) for col in columns.keys()}
        else:
            column_names = set()

        path = info.get("path") or info.get("tabla")
        canonical = normalize_identifier(table_key)

        aliases = {canonical}
        if isinstance(path, str):
            normalized_path = normalize_identifier(path)
            if normalized_path:
                aliases.add(normalized_path)
                path_parts = normalized_path.split(".")
                if path_parts:
                    aliases.add(path_parts[-1])
                if len(path_parts) >= 2:
                    aliases.add(".".join(path_parts[-2:]))

        entry = {
            "name": canonical,
            "columns": column_names,
            "aliases": aliases,
        }

        for alias in aliases:
            catalog[alias] = entry

    return catalog


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
