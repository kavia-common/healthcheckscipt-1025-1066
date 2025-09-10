from typing import Any, Dict, Optional
import logging

from .logging_utils import LogContext, new_trace_id, log_with
from . import config as _config
from ..main import run_cli  # type: ignore

def _airflow_overrides(cfg: _config.AppConfig, environment: Optional[str]) -> Dict[str, Any]:
    overrides: Dict[str, Any] = {}
    if environment:
        overrides["environment"] = environment
    overrides["namespace"] = cfg.namespace
    overrides["node_label_selector"] = cfg.node_label_selector
    overrides["pod_label_selector"] = cfg.pod_label_selector
    return overrides


# PUBLIC_INTERFACE
def run_healthcheck(site_id: Optional[str] = None, environment: Optional[str] = None) -> Dict[str, Any]:
    """Airflow-compatible callable to execute DU health checks without HTTP."""
    cfg = _config.AppConfig()
    cfg.load_from_files_and_env(env_override=environment)
    overrides = _airflow_overrides(cfg, environment)
    trace = new_trace_id()
    logger = logging.getLogger("du-healthcheck")
    with LogContext(trace_id=trace, site_id=site_id or cfg.site_id, environment=environment or cfg.environment, namespace=cfg.namespace):
        log_with(logger, logging.INFO, event="airflow_callable_start")
        result = run_cli(site_id=site_id, environment=environment, overrides=overrides)  # type: ignore
        log_with(logger, logging.INFO, event="airflow_callable_end", status=result.get("status"))
        return result
