"""Prompt-building helpers for the Crew orchestrator."""
from __future__ import annotations

from typing import Dict, List, Optional


def _build_interpreter_prompt(
    user_message: str, history_text: str, has_history: bool
) -> str:
    """Compose the system prompt sent to the interpreter agent."""
    base = [
        "Analiza la intención del usuario y determina si requiere una consulta SQL.",
    ]
    if has_history:
        base.append(
            "Utiliza el historial proporcionado utilizando el tool para tomar la decision"
        )
        base.append("Historial:")
        base.append(history_text)
    else:
        base.append(
            "Trabaja únicamente con el mensaje actual; No utilices la tool ya que el historial esta vacio"
        )

    base.append("")
    base.append(f"Mensaje actual: {user_message}")
    base.append("")
    base.append("Responde exclusivamente en JSON con las claves:")
    base.append("- requires_sql: true o false")
    base.append("- reasoning: explicación corta")
    base.append("- refined_question: reformulación clara de la solicitud")

    return "\n".join(base)


def _build_sql_prompt(
    refined_question: str,
    metadata_summary: str,
    interpreter_data: Dict[str, object],
) -> str:
    """Create the instruction block used by the SQL generator agent."""
    base = [
        "Genera una consulta SQL siguiendo el BigQuery Standard SQL que responda la pregunta.",
        "Utiliza solo tablas y columnas disponibles en los metadatos y respeta todos los filtros implícitos en la solicitud.",
        f"Pregunta refinada: {refined_question}",
        f"Contexto adicional: {interpreter_data.get('reasoning', '')}",
        "",
        "Metadatos disponibles:",
        metadata_summary,
        "",
    ]

    base.append("")
    base.append("Responde en JSON con las claves:")
    base.append("- sql: cadena con la consulta o null si no es necesaria")
    base.append(
        "- sql_explicacion: Explica la consulta con detalle, con agregaciones, filtros etc si los hay. No menciones directamente el nombre literal de la columna en metadata"
    )
    base.append(
        "- semantics: objeto con las siguientes claves, completa con el nombre usado en el esquema obtenido con el select:"
    )
    base.append(
        " - aggregated_periods ([str|null]): periodos principales ('año', 'mes', 'semana', 'día')."
    )
    base.append(
        " - aggregated_labels ([str|null]): lista de categorías de agrupación, si aplica."
    )

    return "\n".join(base)


def _build_executor_prompt(
    user_message: str,
    sql: Optional[str],
    interpreter_data: Dict[str, object],
) -> str:
    """Generate instructions for the executor agent running BigQuery."""
    base = [
        "Eres el agente ejecutor. Recibiste una consulta SQL que ya fue validada."
        " Debes ejecutarla usando exclusivamente el tool `bigquery_sql_runner`.",
    ]
    base.append(f"Mensaje original del usuario: {user_message}")
    base.append(f"Análisis del intérprete: {interpreter_data.get('reasoning', '')}")
    if sql:
        base.append("Consulta SQL a ejecutar:")
        base.append(f"```sql\n{sql}\n```")
        base.append(
            "Ejecuta el tool una sola vez y devuelve un resumen breve del resultado "
            "en formato JSON con las claves status (success/error) y detail."
        )
    else:
        base.append(
            "No hay consulta SQL que ejecutar. Responde con JSON {\"status\": \"skipped\"}."
        )
    base.append(
        "Si el tool devuelve un error, refleja ese error en la clave detail del JSON."
    )
    base.append("No realices interpretaciones ni ofrezcas conclusiones analíticas.")
    return "\n\n".join(base)


def _build_validator_prompt(
    sql: str,
    refined_question: str,
) -> str:
    """Prepare the validation prompt that guards SQL safety."""
    return (
        "Evalúa la sentencia SQL propuesta antes de su ejecución. Debes usar el tool"
        " `sql_validation_tool` para verificar que sea segura.\n"
        f"Consulta propuesta:\n```sql\n{sql}\n```\n"
        f"Pregunta del usuario: {refined_question}\n"
        "Responde exclusivamente en JSON con las claves: valid (bool), message,"
        " sanitized_sql, issues (lista) y warnings (lista)."
    )


def _build_analyzer_prompt(
    refined_question: str,
    sql: str | None,
    sql_explicacion: str | None,
    rows: List[Dict[str, object]] | None,
    semantics: Dict[str, object],
) -> str:
    """Build the prompt that guides the Gemini-powered analysis agent with automatic chart selection."""
    base = [
        "Analiza los resultados devueltos por BigQuery y responde en español claro a la pregunta.",
        "La respuesta debe tener como máximo dos frases, precisas y basadas exclusivamente en los resultados.",
        "Utiliza el tool `gemini_result_analyzer` para generar el resumen narrativo.",
        "El resultado final debe ser JSON con las claves `text` y `chart` (esta última puede ser null).",
    ]

    aggregated_period = semantics.get("aggregated_periods", []) or []
    aggregated_labels = semantics.get("aggregated_labels", []) or []

    if aggregated_period:
        base.append(
            "Si hay periodos agregados en los resultados, asegúrate de mencionar el principal antes que cualquier desglose."
        )
    if aggregated_labels:
        base.append(
            "Cuando existan categorías de agrupación relevantes, destácalas solo si aportan a la respuesta."
        )

    if sql:
        base.append("Consulta SQL ejecutada:")
        base.append(f"```sql\n{sql}\n```")

    base.append(f"Pregunta a resolver: {refined_question}")
    if sql_explicacion:
        base.append(f"Incluye la explicación de la consulta: {sql_explicacion}")
    else:
        base.append("No se proporcionó explicación de la consulta; responde solo con base en los resultados.")

    return "\n\n".join(base)


# Public aliases without underscores for convenient imports.
build_interpreter_prompt = _build_interpreter_prompt
build_sql_prompt = _build_sql_prompt
build_executor_prompt = _build_executor_prompt
build_validator_prompt = _build_validator_prompt
build_analyzer_prompt = _build_analyzer_prompt

__all__ = [
    "_build_interpreter_prompt",
    "_build_sql_prompt",
    "_build_executor_prompt",
    "_build_validator_prompt",
    "_build_analyzer_prompt",
    "build_interpreter_prompt",
    "build_sql_prompt",
    "build_executor_prompt",
    "build_validator_prompt",
    "build_analyzer_prompt",
]
