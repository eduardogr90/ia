"""Prompt-building helpers for the Crew orchestrator."""
from __future__ import annotations

from typing import Dict, List, Optional

from .semantics import coerce_bool


def _build_interpreter_prompt(
    user_message: str, history_text: str, has_history: bool
) -> str:
    """Compose the system prompt sent to the interpreter agent."""
    base = [
        "Analiza la intención del usuario y determina si requiere una consulta SQL.",
    ]
    if has_history:
        base.append(
            "Utiliza el historial proporcionado solo cuando aporte contexto relevante y no hagas suposiciones ajenas a él."
        )
        base.append("Historial:")
        base.append(history_text)
    else:
        base.append(
            "Trabaja únicamente con el mensaje actual; no menciones ni supongas mensajes anteriores."
        )
    base.append("")
    base.append(f"Mensaje actual: {user_message}")
    base.append("")
    base.append("Responde exclusivamente en JSON con las claves:")
    base.append("- requires_sql: true o false")
    base.append("- reasoning: explicación corta")
    base.append("- refined_question: reformulación clara de la solicitud, si no es comparativa ni evolutiva y agregated period solo tiene un valor, reformula con una de una unidad inferior (Año->Mes->semana->Dia)")
    base.append(
        "- semantics: objeto con is_comparative (bool), wants_visual (bool), aggregated_period (string o null), aggregated_label (string o null) y breakdown_unit (string o null)"
    )
    return "\n".join(base)


def _build_sql_prompt(
    refined_question: str,
    metadata_summary: str,
    interpreter_data: Dict[str, object],
    semantics: Dict[str, object],
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

    is_comparative = coerce_bool(semantics.get("is_comparative"))
    aggregated_period = (
        semantics.get("aggregated_period")
        if isinstance(semantics.get("aggregated_period"), str)
        else None
    )
    aggregated_label = (
        semantics.get("aggregated_label")
        if isinstance(semantics.get("aggregated_label"), str)
        else None
    )
    breakdown_unit = (
        semantics.get("breakdown_unit")
        if isinstance(semantics.get("breakdown_unit"), str)
        else None
    )

    if is_comparative:
        base.append(
            "La pregunta es comparativa o evolutiva. Mantén exactamente la granularidad indicada por el usuario y no añadas desgloses adicionales."
        )
    elif aggregated_period:
        period_label = aggregated_label or "solicitado"
        breakdown_unit_label = breakdown_unit or "más pequeño"
        base.append(
            "La solicitud pide un agregado "
            f"{period_label}. Además del total requerido, incorpora en la consulta un desglose {breakdown_unit_label} "
            "del mismo periodo solo con fines analíticos."
        )
        base.append(
            "Puedes usar CTEs, UNION ALL o GROUPING SETS para entregar ambos niveles en un mismo resultado, identificando cada fila con una columna que indique el nivel de agregación (por ejemplo, nivel_agregacion)."
        )
        base.append(
            "No alteres la métrica original ni cambies los filtros solicitados. Si el desglose adicional no es viable con los metadatos disponibles, indícalo con claridad en analysis."
        )
    else:
        base.append(
            "Respeta la granularidad mencionada por el usuario y evita añadir niveles temporales no pedidos."
        )

    base.append("")
    base.append("Responde en JSON con las claves:")
    base.append("- sql: cadena con la consulta o null si no es necesaria")
    base.append(
        "- analysis: explicación breve de la estrategia e indica cualquier decisión sobre granularidad"
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
    rows: List[Dict[str, object]] | None,
    semantics: Dict[str, object],
) -> str:
    """Build the prompt that guides the Gemini-powered analysis agent."""
    base = [
        "Analiza los resultados devueltos por BigQuery y responde en español claro a la pregunta.",
        "Responde primero a la métrica o total solicitado exactamente como lo pidió el usuario.",
        "Si solo hay un valor disponible, limita la respuesta a una frase directa y concreta.",
        "Sé preciso, conciso y basa tus conclusiones únicamente en los resultados mostrados.",
        "Debes usar el tool `gemini_result_analyzer` para generar el resumen narrativo.",
    ]
    is_comparative = coerce_bool(semantics.get("is_comparative"))
    aggregated_period = (
        semantics.get("aggregated_period")
        if isinstance(semantics.get("aggregated_period"), str)
        else None
    )
    aggregated_label = (
        semantics.get("aggregated_label")
        if isinstance(semantics.get("aggregated_label"), str)
        else None
    )
    breakdown_unit = (
        semantics.get("breakdown_unit")
        if isinstance(semantics.get("breakdown_unit"), str)
        else None
    )
    wants_visual = coerce_bool(semantics.get("wants_visual"))

    if is_comparative:
        base.append(
            "La solicitud es comparativa o evolutiva; responde siguiendo esa estructura y evita agregar desgloses extra."
        )
    elif aggregated_period:
        period_label = aggregated_label or "principal"
        breakdown_unit_label = breakdown_unit or "secundario"
        base.append(
            "Presenta el total "
            f"{period_label} primero y, de forma opcional y breve, comenta hallazgos relevantes del desglose {breakdown_unit_label}."
        )
    base.append(
        "Cuando existan varios registros, asume que la interfaz mostrará una tabla con los totales relevantes; no describas columnas irrelevantes."
    )
    if wants_visual:
        base.append(
            "El usuario solicitó una visualización. Puedes sugerirla brevemente solo si los datos lo justifican; de lo contrario, mantén chart en null."
        )
    else:
        base.append(
            "El usuario no pidió gráficos; mantén chart en null y no propongas visualizaciones."
        )
    if sql:
        base.append("Consulta SQL ejecutada:")
        base.append(f"```sql\n{sql}\n```")
    base.append(
        f"Cantidad de filas disponibles: {len(rows) if rows else 0}. Usa el tool para obtener la respuesta final."
    )
    base.append(
        "El resultado final debe ser JSON con las claves text y chart (esta última puede ser null)."
    )
    base.append(f"Pregunta a resolver: {refined_question}")
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
