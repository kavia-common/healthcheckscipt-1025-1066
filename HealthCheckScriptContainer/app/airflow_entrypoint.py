from typing import Any, Dict, Optional

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
    # Import here to avoid circular or heavy imports during module load in Airflow scheduler
    from . import config as _config
    from ..main import run_cli  # type: ignore

    overrides = {}
    cfg = _config.AppConfig()
    if environment:
        overrides["environment"] = environment
    if cfg:
        # respect env-configured defaults like namespace/selectors
        overrides["namespace"] = cfg.namespace
        overrides["node_label_selector"] = cfg.node_label_selector
        overrides["pod_label_selector"] = cfg.pod_label_selector

    return run_cli(site_id=site_id, environment=environment, overrides=overrides)  # type: ignore
