from typing import Any, Dict, Optional
import logging

from .logging_utils import LogContext, new_trace_id, log_with
from . import config as _config
from ..main import run_cli  # type: ignore

# PUBLIC_INTERFACE
def run_healthcheck(site_id: Optional[str] = None, environment: Optional[str] = None) -> Dict[str, Any]:
    """Airflow-compatible callable to execute DU health checks without HTTP.

    This function simply defers to the standalone CLI/main implementation to
    keep import paths stable for existing DAGs:

        from app.airflow_entrypoint import run_healthcheck

    Args:
        site_id: Optional site identifier to include in the report.
        environment: Optional environment override (dev|stage|prod).

    Returns:
        Dict[str, Any]: Structured health report dictionary.
    """
    overrides: Dict[str, Any] = {}
    cfg = _config.AppConfig()
    cfg.load_from_files_and_env(env_override=environment)

    if environment:
        overrides["environment"] = environment
    if cfg:
        overrides["namespace"] = cfg.namespace
        overrides["node_label_selector"] = cfg.node_label_selector
        overrides["pod_label_selector"] = cfg.pod_label_selector

    trace = new_trace_id()
    logger = logging.getLogger("du-healthcheck")
    with LogContext(trace_id=trace, site_id=site_id or cfg.site_id, environment=environment or cfg.environment, namespace=cfg.namespace):
        log_with(logger, logging.INFO, event="airflow_callable_start")
        result = run_cli(site_id=site_id, environment=environment, overrides=overrides)  # type: ignore
        log_with(logger, logging.INFO, event="airflow_callable_end", status=result.get("status"))
        return result
