"""Shared utilities for CrewAI agents and tools."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError


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

        table_name = (
            data.get("table")
            or data.get("tabla")
            or path.stem
        )
        if not table_name:
            continue

        normalized_name = str(table_name)
        table_payload = data
        if isinstance(data, dict):
            nested_payload = data.get(normalized_name)
            if isinstance(nested_payload, dict):
                table_payload = nested_payload

        normalized_entry = _normalize_table_metadata(
            normalized_name,
            table_payload if isinstance(table_payload, dict) else {},
            raw_payload=data if isinstance(data, dict) else {},
        )
        metadata[normalized_name] = normalized_entry
    return metadata


def _normalize_table_metadata(
    table_name: str,
    table_payload: Dict[str, Any],
    *,
    raw_payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Standardise table metadata regardless of the JSON schema."""

    normalized: Dict[str, Any] = {
        key: value for key, value in table_payload.items()
    }
    normalized.setdefault("name", table_name)

    description = normalized.get("description")
    if isinstance(description, list):
        description = _normalize_description(description)
    if not description:
        description = _normalize_description(
            table_payload.get("descripcion")
        )
    if description:
        normalized["description"] = description

    path_value = (
        normalized.get("path")
        or normalized.get("tabla")
        or raw_payload.get("path")
        or raw_payload.get("tabla")
    )
    if path_value:
        normalized["path"] = path_value

    columns_obj = normalized.get("columns")
    if not columns_obj:
        columns_obj = normalized.get("columnas") or table_payload.get("columnas")

    normalized_columns = _normalize_columns(columns_obj)
    if normalized_columns:
        normalized["columns"] = normalized_columns
        normalized.setdefault("columnas", normalized_columns)

    return normalized


def _normalize_columns(columns_obj: Any) -> Dict[str, Dict[str, Any]]:
    """Return a dict keyed by column name with harmonised attributes."""

    normalized: Dict[str, Dict[str, Any]] = {}

    if isinstance(columns_obj, dict):
        items = columns_obj.items()
    elif isinstance(columns_obj, list):
        items = []
        for column in columns_obj:
            if isinstance(column, dict) and column.get("name"):
                items.append((column["name"], column))
    else:
        items = []

    for raw_name, raw_details in items:
        column_name = str(raw_name)
        normalized[column_name] = _normalize_column_details(raw_details)

    return normalized


def _normalize_column_details(raw_details: Any) -> Dict[str, Any]:
    """Normalise column metadata to expose consistent keys."""

    if isinstance(raw_details, dict):
        column_entry = {key: value for key, value in raw_details.items()}
    else:
        column_entry = {"description": _normalize_description(raw_details)}

    description = column_entry.get("description")
    if isinstance(description, list):
        description = _normalize_description(description)
    if not description:
        description = _normalize_description(column_entry.get("descripcion"))
    if description:
        column_entry["description"] = description

    data_type = column_entry.get("type")
    if not data_type:
        data_type = column_entry.get("tipo_dato")
    if data_type:
        column_entry["type"] = data_type

    synonyms = column_entry.get("synonyms")
    if not synonyms:
        synonyms = column_entry.get("sinonimos")
    if synonyms:
        if isinstance(synonyms, (list, tuple, set)):
            cleaned = [
                str(item).strip()
                for item in synonyms
                if str(item).strip()
            ]
        else:
            text = str(synonyms).strip()
            cleaned = [text] if text else []
        column_entry["synonyms"] = cleaned

    return column_entry


def _normalize_description(raw_value: Any) -> str:
    """Convert description fields into a plain string."""

    if isinstance(raw_value, list):
        return "\n".join(
            line.strip()
            for line in raw_value
            if isinstance(line, str) and line.strip()
        )
    if raw_value is None:
        return ""
    return str(raw_value).strip()


__all__ = [
    "analyze_tables",
    "build_metadata_catalog",
    "collect_column_issues",
    "expression_name",
    "is_select_statement",
    "load_model_metadata",
    "log_sql_audit",
    "normalize_identifier",
    "extract_table_alias",
]
