"""Text normalization and semantic helpers for the Crew orchestrator."""
from __future__ import annotations

import unicodedata
from typing import Dict, List


def _normalize_text(text: str | None) -> str:
    """Normalize text for semantic analysis removing accents and case."""
    if not text:
        return ""
    normalized = unicodedata.normalize("NFD", text)
    without_marks = "".join(
        char for char in normalized if unicodedata.category(char) != "Mn"
    )
    return without_marks.lower()


def _analyze_question_semantics(question: str) -> Dict[str, object]:
    """Derive high-level semantic hints from the raw user question."""
    normalized = _normalize_text(question)
    is_comparative = any(
        keyword in normalized
        for keyword in (
            " vs ",
            "vs.",
            "compar",
            "diferenc",
            "respecto",
            "frente a",
            "variac",
            "evolu",
            "tendenc",
            "increment",
            "disminu",
        )
    )
    wants_visual = any(
        keyword in normalized for keyword in ("graf", "visualiz", "chart", "diagrama")
    )

    iteration_patterns = (
        "por mes",
        "por trimestre",
        "por ano",
        "por año",
        "por semana",
        "por dia",
        "por día",
        "mes a mes",
        "trimestre a trimestre",
        "semana a semana",
        "dia a dia",
        "día a día",
        "mensualmente",
        "trimestralmente",
        "semanalmente",
        "diariamente",
    )
    has_iteration = any(pattern in normalized for pattern in iteration_patterns)

    month_names = (
        "enero",
        "febrero",
        "marzo",
        "abril",
        "mayo",
        "junio",
        "julio",
        "agosto",
        "septiembre",
        "setiembre",
        "octubre",
        "noviembre",
        "diciembre",
    )

    period_candidates: List[str] = []
    if not is_comparative and not has_iteration:
        if any(name in normalized for name in month_names) or any(
            keyword in normalized
            for keyword in (
                " del mes ",
                "en el mes ",
                "durante el mes",
                "ultimo mes",
                "último mes",
                "mes pasado",
            )
        ):
            period_candidates.append("monthly")
        if "trimestre" in normalized or "trimestr" in normalized:
            period_candidates.append("quarterly")
        if any(
            keyword in normalized
            for keyword in (
                " ano ",
                " año ",
                " anual",
                "durante 20",
                "en 20",
                "del 20",
            )
        ):
            period_candidates.append("yearly")

    breakdown_blockers = {
        "monthly": ("semana", "semanal", "dia", "día", "diario"),
        "quarterly": ("mes", "mensual", "semana", "semanal"),
        "yearly": ("mes", "mensual", "trimestre", "trimestr", "semana", "semanal"),
    }

    aggregated_period = None
    for candidate in period_candidates:
        blockers = breakdown_blockers.get(candidate, ())
        if any(blocker in normalized for blocker in blockers):
            continue
        aggregated_period = candidate
        break

    period_labels = {
        "monthly": ("mensual", "semanal"),
        "quarterly": ("trimestral", "mensual"),
        "yearly": ("anual", "trimestral"),
    }
    aggregated_label, breakdown_unit = (None, None)
    if aggregated_period:
        aggregated_label, breakdown_unit = period_labels.get(aggregated_period, (None, None))

    return {
        "normalized": normalized,
        "is_comparative": is_comparative,
        "wants_visual": wants_visual,
        "aggregated_period": aggregated_period,
        "aggregated_label": aggregated_label,
        "breakdown_unit": breakdown_unit,
    }


def _coerce_bool(value: object) -> bool:
    """Coerce heterogeneous values into booleans for semantics payloads."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"true", "1", "si", "sí", "yes"}
    if isinstance(value, (int, float)):
        return value != 0
    return False


def _extract_semantics(interpreter_data: Dict[str, object]) -> Dict[str, object]:
    """Merge semantic hints coming from the interpreter agent output."""
    semantics: Dict[str, object] = {}
    raw_semantics = interpreter_data.get("semantics")
    if isinstance(raw_semantics, dict):
        semantics.update(raw_semantics)

    for key in (
        "is_comparative",
        "aggregated_period",
        "aggregated_label",
        "breakdown_unit",
        "wants_visual",
    ):
        if key not in semantics and key in interpreter_data:
            semantics[key] = interpreter_data[key]

    semantics["is_comparative"] = _coerce_bool(semantics.get("is_comparative"))
    semantics["wants_visual"] = _coerce_bool(semantics.get("wants_visual"))
    semantics["aggregated_period"] = (
        semantics.get("aggregated_period")
        if isinstance(semantics.get("aggregated_period"), str)
        else None
    )
    semantics["aggregated_label"] = (
        semantics.get("aggregated_label")
        if isinstance(semantics.get("aggregated_label"), str)
        else None
    )
    semantics["breakdown_unit"] = (
        semantics.get("breakdown_unit")
        if isinstance(semantics.get("breakdown_unit"), str)
        else None
    )

    return semantics


# Backwards-compatible aliases without the underscore prefix.
normalize_text = _normalize_text
analyze_question_semantics = _analyze_question_semantics
coerce_bool = _coerce_bool
extract_semantics = _extract_semantics

__all__ = [
    "_normalize_text",
    "_analyze_question_semantics",
    "_coerce_bool",
    "_extract_semantics",
    "normalize_text",
    "analyze_question_semantics",
    "coerce_bool",
    "extract_semantics",
]
