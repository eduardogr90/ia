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

        def _format_description(value: object) -> str:
            if isinstance(value, list):
                return "\n".join(
                    line.strip()
                    for line in value
                    if isinstance(line, str) and line.strip()
                )
            if value is None:
                return ""
            return str(value).strip()

        def _format_synonyms(value: object) -> str:
            if isinstance(value, (list, tuple, set)):
                cleaned = [str(item).strip() for item in value if str(item).strip()]
                return ", ".join(cleaned)
            if value is None:
                return ""
            text = str(value).strip()
            return text

        sections = []
        for table, table_data in self.metadata.items():
            if not isinstance(table_data, dict):
                continue
            description = (
                table_data.get("description")
                or table_data.get("descripcion")
            )
            description_text = _format_description(description)

            columns_obj = (
                table_data.get("columns")
                or table_data.get("columnas")
                or {}
            )

            column_lines = []
            if isinstance(columns_obj, dict):
                for col_name, col_info in columns_obj.items():
                    column_desc = ""
                    column_type = ""
                    column_synonyms = ""
                    if isinstance(col_info, dict):
                        column_desc = _format_description(
                            col_info.get("description")
                            or col_info.get("descripcion")
                        )
                        column_type = str(
                            col_info.get("type") or col_info.get("tipo_dato") or ""
                        ).strip()
                        column_synonyms = _format_synonyms(
                            col_info.get("synonyms")
                            or col_info.get("sinonimos")
                        )
                    else:
                        column_desc = _format_description(col_info)

                    details = []
                    if column_desc:
                        details.append(column_desc)
                    if column_type:
                        details.append(f"Tipo: {column_type}")
                    if column_synonyms:
                        details.append(f"Sinónimos: {column_synonyms}")

                    if details:
                        column_lines.append(f"- {col_name}: " + " | ".join(details))
                    else:
                        column_lines.append(f"- {col_name}")

            section_lines = [f"Tabla: {table}"]
            if description_text:
                section_lines.append(f"Descripción: {description_text}")
            if column_lines:
                section_lines.append("Columnas:\n" + "\n".join(column_lines))
            sections.append("\n".join(section_lines))

        return "\n\n".join(sections)

    def _run(self, table: str | None = None) -> str:
        if not self.metadata:
            return "{}"
        if table and table in self.metadata:
            return json.dumps(self.metadata[table], ensure_ascii=False, indent=2)
        return json.dumps(self.metadata, ensure_ascii=False, indent=2)
