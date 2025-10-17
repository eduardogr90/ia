"""Validator agent ensuring SQL statements comply with policies."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from crewai import Agent
from crewai.tools import BaseTool
from pydantic import Field

from .agents_utils import validate_sql_statement


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


__all__ = ["SQLValidationTool", "create_validator_agent"]
