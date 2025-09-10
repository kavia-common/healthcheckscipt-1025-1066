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

    overrides: Dict[str, Any] = {}
    cfg = _config.AppConfig()
    # Load to pick up YAML + env for defaults (namespace/selectors) using provided environment
    cfg.load_from_files_and_env(env_override=environment)

    if environment:
        overrides["environment"] = environment
    if cfg:
        overrides["namespace"] = cfg.namespace
        overrides["node_label_selector"] = cfg.node_label_selector
        overrides["pod_label_selector"] = cfg.pod_label_selector

    return run_cli(site_id=site_id, environment=environment, overrides=overrides)  # type: ignore
