"""Validator agent ensuring SQL statements comply with policies."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from crewai import Agent
from crewai.tools import BaseTool
from pydantic import Field

from .agents_utils import log_sql_audit


class SQLValidationTool(BaseTool):
    """Applies deterministic validation rules to generated SQL statements."""

    name: str = "sql_validation_tool"
    description: str = (
        "Valida una consulta SQL para asegurar que solo se realicen operaciones de lectura, "
        "que se respeten los límites y que las tablas y columnas existan en el modelo."
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)
    allowed_tables: List[str] = Field(default_factory=list)
    llm: Any | None = Field(default=None, exclude=True, repr=False)
    max_limit: int = Field(default=1000)
    audit_path: Path = Field(
        default=Path(__file__).resolve().parent.parent
        / "data"
        / "logs"
        / "sql_audit.json"
    )
    candidate_sql: str = Field(default="")
    question: str = Field(default="")

    def set_metadata(self, metadata: Dict[str, Any]) -> None:
        self.metadata = metadata or {}
        table_names: set[str] = set()
        for key, value in self.metadata.items():
            table_names.add(str(key))
            if isinstance(value, dict):
                path = value.get("path") or value.get("tabla")
                if isinstance(path, str) and path:
                    table_names.add(path)
                inner = value.get(key)
                if isinstance(inner, dict):
                    inner_path = inner.get("path") or inner.get("tabla")
                    if isinstance(inner_path, str) and inner_path:
                        table_names.add(inner_path)
        self.allowed_tables = sorted(filter(None, table_names))

    def set_candidate(self, sql: str, question: str | None = None) -> None:
        self.candidate_sql = sql or ""
        self.question = question or ""

    def set_llm(self, llm: Any | None) -> None:
        """Attach the shared LLM used to run the validation prompts."""

        self.llm = llm

    # ------------------------------------------------------------------
    def _build_prompt(self, sql: str) -> str:
        """Create the instruction set for the LLM-based validation."""

        lines: List[str] = [
            "Eres un auditor de seguridad que revisa sentencias SQL para BigQuery.",
            "Debes confirmar que la sentencia cumple el estándar SQL de BigQuery y las siguientes políticas:",
            "- Solo se permiten consultas de lectura (SELECT, WITH, subconsultas y funciones analíticas).",
            "- No permitas DDL ni DML (INSERT, UPDATE, DELETE, MERGE, CREATE, DROP, ALTER, TRUNCATE).",
            "- No permitas comentarios, múltiples sentencias ni comandos que puedan modificar datos.",
            f"- Aplica un LIMIT máximo de {self.max_limit} filas cuando sea necesario.",
        ]
        if self.allowed_tables:
            lines.append("- Las tablas autorizadas por el modelo son:")
            for name in self.allowed_tables:
                lines.append(f"  * {name}")
            lines.append(
                "  Si la consulta hace referencia a una tabla distinta, márcalo como un problema."
            )
        lines.append(
            "Analiza la consulta y responde estrictamente en JSON con las claves: valid (bool),"
            " message (string), sanitized_sql (string o null), issues (lista de strings) y warnings"
            " (lista de strings)."
        )
        lines.append(
            "- Si consideras que la consulta es segura, establece valid=true y devuelve en"
            " sanitized_sql la versión lista para BigQuery sin comentarios ni punto y coma final."
        )
        lines.append(
            "- Si necesitas ajustar detalles menores (por ejemplo retirar el punto y coma final o"
            " agregar LIMIT), explica la modificación en warnings."
        )
        lines.append(
            "- Cuando detectes algún problema, establece valid=false, incluye los motivos en issues"
            " y usa sanitized_sql=null."
        )
        if self.question:
            lines.append(f"Pregunta del usuario: {self.question}")
        lines.append("Consulta a evaluar:")
        lines.append(f"```sql\n{sql}\n```")
        lines.append("Devuelve únicamente el JSON solicitado sin explicaciones adicionales.")
        return "\n".join(lines)

    def _run(self, sql: str | None = None) -> str:
        statement = (sql or self.candidate_sql or "").strip()
        issues: List[str] = []
        warnings: List[str] = []
        sanitized_sql: str | None = None
        valid = False
        message = ""

        if not statement:
            issues.append("La sentencia SQL está vacía.")
            message = "La consulta fue rechazada por el validador."
        elif self.llm is None:
            issues.append("No hay un modelo LLM disponible para validar la consulta.")
            message = "No fue posible validar la consulta SQL."
        else:
            prompt = self._build_prompt(statement)
            try:
                response = self.llm.invoke(prompt)
                raw_text = (
                    response.content  # type: ignore[attr-defined]
                    if hasattr(response, "content")
                    else str(response)
                )
            except Exception as exc:  # pragma: no cover - depende del entorno
                issues.append(f"Error al solicitar la validación al modelo: {exc}")
                message = "No fue posible validar la consulta SQL."
            else:
                payload: Dict[str, Any] | None = None
                try:
                    payload = json.loads(raw_text)
                except json.JSONDecodeError:
                    start = raw_text.find("{")
                    end = raw_text.rfind("}")
                    if start != -1 and end != -1 and end > start:
                        try:
                            payload = json.loads(raw_text[start : end + 1])
                        except json.JSONDecodeError:
                            payload = None
                if isinstance(payload, dict):
                    valid = bool(payload.get("valid", False))
                    message = str(payload.get("message") or "").strip()
                    sanitized_raw = payload.get("sanitized_sql")
                    if isinstance(sanitized_raw, str):
                        sanitized_sql = sanitized_raw.strip()
                    elif sanitized_raw is None:
                        sanitized_sql = None
                    issues_list = payload.get("issues")
                    if isinstance(issues_list, list):
                        issues = [str(item).strip() for item in issues_list if str(item).strip()]
                    warnings_list = payload.get("warnings")
                    if isinstance(warnings_list, list):
                        warnings = [
                            str(item).strip() for item in warnings_list if str(item).strip()
                        ]
                    if valid and not sanitized_sql:
                        valid = False
                        issues.append(
                            "El modelo no devolvió una versión lista para ejecutar en sanitized_sql."
                        )
                    if not message:
                        message = (
                            "Consulta validada correctamente." if valid else "La consulta fue rechazada por el validador."
                        )
                else:
                    issues.append("El modelo no devolvió un JSON válido con el resultado de la validación.")
                    message = "No fue posible validar la consulta SQL."

        if not valid:
            sanitized_sql = None
            if not message:
                message = "La consulta fue rechazada por el validador."
        else:
            if not message:
                message = "Consulta validada correctamente."

        result = {
            "valid": valid,
            "sanitized_sql": sanitized_sql,
            "issues": issues,
            "warnings": warnings,
            "message": message,
            "question": self.question,
        }

        log_sql_audit(
            self.audit_path,
            {
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                "question": self.question,
                "submitted_sql": sql or self.candidate_sql,
                "sanitized_sql": sanitized_sql,
                "valid": valid,
                "issues": issues,
                "warnings": warnings,
            },
        )

        return json.dumps(result, ensure_ascii=False)


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
        verbose=False,
        tools=[validation_tool],
        llm=llm,
    )


__all__ = ["SQLValidationTool", "create_validator_agent"]
