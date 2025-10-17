"""Tool exposing relational model metadata for SQL generation."""
from __future__ import annotations

import json
from typing import Any, Dict

from crewai.tools import BaseTool
from pydantic import Field


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
