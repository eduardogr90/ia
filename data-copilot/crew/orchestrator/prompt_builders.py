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
    base.append("- refined_question: reformulación clara de la solicitud. Si el usuario pide un periodo de tiempo y no se trata de una solicitud comparativa ni evolutiva, incluye un desglose temporal un nivel más detallado (Año→Mes→Semana→Día).")
    base.append("- semantics: objeto con las siguientes claves:")
    base.append("    - is_comparative (bool): si compara periodos o categorías.")
    base.append("    - wants_visual (bool): si desea una visualización o gráfico.")
    base.append("    - aggregated_period (str|null): periodo principal ('año', 'mes', 'semana', 'día').")
    base.append("    - aggregated_labels ([str|null]): lista categorías de agrupación ('proyecto', 'cliente', 'categoría').")
    base.append("    - breakdown_units ([str|null]): lista unidades de desglose adicional (por ejemplo, 'semana' dentro de un mes).")
    base.append("    - output_type ([str]): tipo de resultado esperado según la intención del usuario:")
    base.append("        * 'value' → valor único o agregado.")
    base.append("        * 'list' → lista simple de elementos.")
    base.append("        * 'table' → tabla con una sola dimensión de agrupación.")
    base.append("        * 'matrix' → cruce bidimensional (si hay 'aggregated_label' y 'breakdown_unit') o dos columnas en aggregated_label.")
    base.append("        * 'visual' → gráfico o visualización.")
    base.append("        * 'otput1+output2' → cuando se solicita una tabla y un gráfico o visualización etc.")

    return "\n".join(base)


def _build_sql_prompt(
    refined_question: str,
    metadata_summary: str,
    interpreter_data: Dict[str, object],
    semantics: Dict[str, object],
) -> str:
    """Create the instruction block used by the SQL generator agent."""
    base = [
        "Genera una consulta SQL siguiendo el BigQuery Standard SQL que responda la pregunta. Se pueden hacer ",
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
            "La pregunta es comparativa o evolutiva. Mantén exactamente la granularidad indicada por el usuario"
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
        "Evalúa la sentencia SQL propuesta si es valida segun el Bigquery SQL Estandar. Recuerda que DAYOFWEEK no es valido\n"
        f"Consulta propuesta:\n```sql\n{sql}\n```\n"
        f"Pregunta del usuario: {refined_question}\n"
        "Responde exclusivamente en JSON con las claves: valid (bool), message,"
        " sanitized_sql, issues (lista) y warnings (lista)."
    )

"""
def _build_analyzer_prompt(
    refined_question: str,
    sql: str | None,
    rows: List[Dict[str, object]] | None,
    semantics: Dict[str, object],
) -> str:

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
    output_type_label = (
        semantics.get("output_type")
        if isinstance(semantics.get("output_type"), str)
        else None
    )


    wants_visual = coerce_bool(semantics.get("wants_visual"))

    if is_comparative:
        base.append(
            "La solicitud es comparativa, responde siguiendo esa estructura"
        )
    elif aggregated_period:
        period_label = aggregated_label or "principal"
        breakdown_unit_label = breakdown_unit or "secundario"
        
    base.append(
        "Cuando existan varios registros, asume que la interfaz mostrará una tabla con los totales relevantes; no describas columnas irrelevantes."
    )
    if wants_visual:
        base.append(
            "Añade{output_type_label} . El usuario solicitó una visualización. Puedes sugerirla brevemente solo si los datos lo justifican; de lo contrario, mantén chart en null."
        )
    else:
        base.append(
            "Añade{output_type_label}. El usuario no pidió gráficos; mantén chart en null y no propongas visualizaciones."
        )
    if sql:
        base.append("Consulta SQL ejecutada:")
        base.append(f"```sql\n{sql}\n```")

    base.append(
        "El resultado final debe ser JSON con las claves text y chart (esta última puede ser null)."
    )
    base.append(f"Pregunta a resolver: {refined_question}")
    return "\n\n".join(base)
"""

def _build_analyzer_prompt(
    refined_question: str,
    sql: str | None,
    rows: List[Dict[str, object]] | None,
    semantics: Dict[str, object],
) -> str:
    """Build the prompt that guides the Gemini-powered analysis agent with automatic chart selection."""
    base = [
        "Analiza los resultados devueltos por BigQuery y responde en español claro a la pregunta.",
        "Responde primero a la métrica o total solicitado exactamente como lo pidió el usuario.",
        "Sé preciso, conciso y basa tus conclusiones únicamente en los resultados mostrados.",
        "Debes usar el tool `gemini_result_analyzer` para generar el resumen narrativo.",
        "El resultado final debe ser JSON con las claves `text` y `chart` (esta última puede ser null).",
    ]

    # === Variables semánticas ===
    is_comparative = bool(semantics.get("is_comparative"))
    wants_visual = bool(semantics.get("wants_visual"))
    aggregated_period = semantics.get("aggregated_period")
    aggregated_labels = semantics.get("aggregated_labels", []) or []
    breakdown_units = semantics.get("breakdown_units", []) or []
    output_type = semantics.get("output_type", "value")

    chart_type = None
    chart_spec = {}

    # === Lógica de selección de gráfico ===

    # 1. Sin visualización
    if not wants_visual:
        chart_type = None
    else:
        # 2. Casos con periodo → gráficos temporales
        if aggregated_period in ["año", "mes", "semana", "día"]:
            if aggregated_labels or is_comparative:
                chart_type = "line_multiple"
                chart_spec = {
                    "x": aggregated_period,
                    "y": "valor",
                    "series": aggregated_labels or ["categoría"],
                }
            else:
                chart_type = "line"
                chart_spec = {
                    "x": aggregated_period,
                    "y": "valor",
                }

        # 3. Casos categóricos sin periodo
        elif aggregated_labels and not aggregated_period:
            if len(aggregated_labels) == 1 and not breakdown_units:
                chart_type = "bar"
                chart_spec = {
                    "x": aggregated_labels[0],
                    "y": "valor",
                }
            elif breakdown_units:
                # Decidir entre barras agrupadas o apiladas
                chart_type = "grouped_bar" if not is_comparative else "stacked_bar"
                chart_spec = {
                    "x": aggregated_labels[0],
                    "y": "valor",
                    "group": breakdown_units[0],
                }
            elif len(aggregated_labels) == 2:
                chart_type = "matrix"
                chart_spec = {
                    "rows": aggregated_labels[0],
                    "cols": aggregated_labels[1],
                    "value": "valor",
                }

        # 4. Composición de un todo (una categoría, pocos elementos)
        if (
            chart_type is None
            and len(aggregated_labels) == 1
            and not breakdown_units
            and output_type in ["visual", "matrix"]
        ):
            chart_type = "pie"
            chart_spec = {"category": aggregated_labels[0], "value": "valor"}

        # 5. Periodo + múltiples labels → matriz o barras agrupadas
        if aggregated_period and aggregated_labels:
            if len(aggregated_labels) >= 2:
                chart_type = "matrix"
                chart_spec = {
                    "rows": aggregated_labels[0],
                    "cols": aggregated_period,
                    "value": "valor",
                }
            else:
                chart_type = "grouped_bar"
                chart_spec = {
                    "x": aggregated_period,
                    "y": "valor",
                    "group": aggregated_labels[0],
                }

        # 6. Valor por defecto si no detecta nada
        if chart_type is None and wants_visual:
            chart_type = "bar"
            chart_spec = {"x": aggregated_labels[0] if aggregated_labels else "categoría", "y": "valor"}

    # === Añadir instrucciones al prompt ===
    if is_comparative:
        base.append("La solicitud es comparativa, responde comparando explícitamente las categorías o periodos.")

    base.append(
        "Cuando existan varios registros, asume que la interfaz mostrará una tabla con los totales relevantes; no describas columnas irrelevantes."
    )

    if wants_visual:
        if chart_type:
            base.append(
                f"El usuario solicitó una visualización. Usa el tipo de gráfico más adecuado según los datos: `{chart_type}`."
            )
            base.append(f"Configura los ejes o dimensiones según:\n{json.dumps(chart_spec, ensure_ascii=False, indent=2)}")
        else:
            base.append("El usuario solicitó una visualización, pero los datos no la justifican; mantén `chart` en null.")
    else:
        base.append("El usuario no pidió gráficos; mantén `chart` en null y no propongas visualizaciones.")

    if sql:
        base.append("Consulta SQL ejecutada:")
        base.append(f"```sql\n{sql}\n```")

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
