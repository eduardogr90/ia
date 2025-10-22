"""Tool exposing relational model metadata for SQL generation."""
from __future__ import annotations

import json
from typing import Any, Dict, Iterable

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

    # ------------------------------------------------------------------
    def set_metadata(self, metadata: Dict[str, Any]) -> None:
        """Replace the metadata dictionary."""

        self.metadata = metadata or {}

    # ------------------------------------------------------------------
    def _extract_table_info(self, table_key: str, table_data: Any) -> Dict[str, Any]:
        """Normalize different metadata formats into a dict with useful details."""

        if not isinstance(table_data, dict):
            return {}

        # Some metadata files wrap the information inside the table key.
        if table_key in table_data and isinstance(table_data[table_key], dict):
            info = table_data[table_key]
        else:
            info = table_data

        return info if isinstance(info, dict) else {}

    def _iter_tables(self) -> Iterable[tuple[str, Dict[str, Any]]]:
        """Yield table names with their normalized info."""

        for table, raw in self.metadata.items():
            info = self._extract_table_info(table, raw)
            if info:
                yield table, info

    def _format_column_entry(self, name: str, payload: Any) -> str:
        """Create a concise description for a column."""

        details: list[str] = []
        if isinstance(payload, dict):
            description = payload.get("description") or payload.get("descripcion")
            if isinstance(description, str) and description.strip():
                details.append(description.strip())
            synonyms = payload.get("synonyms") or payload.get("sinonimos")
            if isinstance(synonyms, (list, tuple)):
                synonym_text = ", ".join(str(item).strip() for item in synonyms if str(item).strip())
                if synonym_text:
                    details.append(f"Sinónimos: {synonym_text}")
            data_type = payload.get("data_type") or payload.get("tipo_dato")
            if isinstance(data_type, str) and data_type.strip():
                details.append(f"Tipo: {data_type.strip()}")

        if details:
            return f"- {name}: " + " | ".join(details)
        return f"- {name}"

    def summary(self) -> str:
        """Return a human readable summary of the available metadata."""

        sections: list[str] = []
        for table, info in self._iter_tables():
            section: list[str] = [f"Tabla: {table}"]

            path = info.get("path") or info.get("tabla")
            if isinstance(path, str) and path.strip():
                section.append(f"Path: {path.strip()}")

            description = info.get("description") or info.get("descripcion")
            if isinstance(description, str) and description.strip():
                section.append(f"Descripción: {description.strip()}")
            elif isinstance(description, list):
                description_lines = [str(item).strip() for item in description if str(item).strip()]
                if description_lines:
                    section.append("Descripción:\n" + "\n".join(f"- {line}" for line in description_lines))

            columns = info.get("columns") or info.get("columnas")
            column_lines: list[str] = []
            if isinstance(columns, dict):
                for col_name, payload in columns.items():
                    column_lines.append(self._format_column_entry(str(col_name), payload))
            elif isinstance(columns, list):
                for payload in columns:
                    if isinstance(payload, dict):
                        name = payload.get("name") or payload.get("nombre")
                        if isinstance(name, str) and name.strip():
                            column_lines.append(self._format_column_entry(name.strip(), payload))
            if column_lines:
                section.append("Columnas:\n" + "\n".join(column_lines))

            sections.append("\n".join(section))

        if not sections:
            return "No hay metadatos disponibles."
        return "\n\n".join(sections)

    def _resolve_table_key(self, table: str) -> str | None:
        """Find the canonical table key that matches the provided identifier."""

        target = table.strip().lower()
        if not target:
            return None

        for table_key, info in self._iter_tables():
            candidates = {table_key.lower()}
            path = info.get("path") or info.get("tabla")
            if isinstance(path, str) and path.strip():
                normalized = path.strip().lower()
                candidates.add(normalized)
                parts = normalized.split(".")
                if parts:
                    candidates.add(parts[-1])
                if len(parts) >= 2:
                    candidates.add(".".join(parts[-2:]))
            alias = info.get("name") or info.get("table")
            if isinstance(alias, str) and alias.strip():
                candidates.add(alias.strip().lower())
            if target in candidates:
                return table_key
        return None

    def _normalized_metadata(self) -> Dict[str, Any]:
        """Return metadata with the nested table key flattened when necessary."""

        normalized: Dict[str, Any] = {}
        for table_key, info in self._iter_tables():
            normalized[table_key] = info
        return normalized

    def _run(self, table: str | None = None) -> str:
        if not self.metadata:
            return "{}"

        if table:
            table_key = self._resolve_table_key(table)
            if table_key:
                info = self._extract_table_info(table_key, self.metadata.get(table_key))
                if info:
                    return json.dumps(info, ensure_ascii=False, indent=2)

        return json.dumps(self._normalized_metadata(), ensure_ascii=False, indent=2)
