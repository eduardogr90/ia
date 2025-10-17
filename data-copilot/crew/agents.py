"""Public entry point aggregating CrewAI agents, tools, and utilities."""
from __future__ import annotations

from importlib import import_module
from pathlib import Path

__path__ = [str(Path(__file__).resolve().parent / "agents")]
if __spec__ is not None:  # pragma: no cover - import system detail
    __spec__.submodule_search_locations = __path__

_agents_utils = import_module(".agents_utils", __name__)
_analyzer = import_module(".analyzer_agent", __name__)
_executor = import_module(".executor_agent", __name__)
_interpreter = import_module(".interpreter_agent", __name__)
_sql_generator = import_module(".sql_generator_agent", __name__)
_tools = import_module(".tools", __name__)
_validator = import_module(".validator_agent", __name__)

ConversationHistoryTool = _tools.ConversationHistoryTool
SQLMetadataTool = _tools.SQLMetadataTool
BigQueryQueryTool = _executor.BigQueryQueryTool
SQLValidationTool = _validator.SQLValidationTool
GeminiAnalysisTool = _analyzer.GeminiAnalysisTool

create_interpreter_agent = _interpreter.create_interpreter_agent
create_sql_generator_agent = _sql_generator.create_sql_generator_agent
create_executor_agent = _executor.create_executor_agent
create_validator_agent = _validator.create_validator_agent
create_analyzer_agent = _analyzer.create_analyzer_agent

normalize_identifier = _agents_utils.normalize_identifier
expression_name = _agents_utils.expression_name
extract_table_alias = _agents_utils.extract_table_alias
analyze_tables = _agents_utils.analyze_tables
collect_column_issues = _agents_utils.collect_column_issues
is_select_statement = _agents_utils.is_select_statement
build_metadata_catalog = _agents_utils.build_metadata_catalog
log_sql_audit = _agents_utils.log_sql_audit
load_model_metadata = _agents_utils.load_model_metadata

__all__ = [
    "ConversationHistoryTool",
    "SQLMetadataTool",
    "BigQueryQueryTool",
    "SQLValidationTool",
    "GeminiAnalysisTool",
    "create_interpreter_agent",
    "create_sql_generator_agent",
    "create_executor_agent",
    "create_validator_agent",
    "create_analyzer_agent",
    "load_model_metadata",
    "normalize_identifier",
    "expression_name",
    "extract_table_alias",
    "analyze_tables",
    "collect_column_issues",
    "is_select_statement",
    "build_metadata_catalog",
    "log_sql_audit",
]
