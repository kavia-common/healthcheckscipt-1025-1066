import json
from typing import Any, Dict

from .config import AppConfig
from .utils import (
    setup_logging,
    fetch_kubeconfig_from_vault,
    build_k8s_client_from_kubeconfig,
    list_nodes,
    list_pods,
    collect_logs_and_metrics,
    tcp_connectivity_check,
    build_health_report,
    send_to_kafka,
    push_to_loki,
)

# PUBLIC_INTERFACE
def run_healthcheck(site_id: str | None = None, environment: str | None = None) -> Dict[str, Any]:
    """Airflow-compatible callable to execute DU health checks without HTTP.

    Args:
        site_id: Optional site identifier to include in the report.
        environment: Optional environment override (dev|stage|prod).

    Returns:
        Dict[str, Any]: Structured health report dictionary.
    """
    cfg = AppConfig()
    if environment:
        cfg.environment = environment
    if site_id:
        cfg.site_id = site_id
    logger = setup_logging(cfg.log_level)
    cfg.validate()

    kubeconfig = fetch_kubeconfig_from_vault(cfg.vault_addr, cfg.vault_token, cfg.vault_kubeconfig_path, timeout=cfg.request_timeout)
    core_api, _ = build_k8s_client_from_kubeconfig(kubeconfig)

    nodes = list_nodes(core_api, cfg.node_label_selector)
    pods = list_pods(core_api, cfg.namespace, cfg.pod_label_selector)
    metrics = collect_logs_and_metrics(core_api, cfg.namespace, pods)
    connectivity = {
        "cu": tcp_connectivity_check(cfg.cu_host, cfg.cu_port, cfg.connectivity_timeout),
        "ru": tcp_connectivity_check(cfg.ru_host, cfg.ru_port, cfg.connectivity_timeout),
    }
    report = build_health_report(cfg.site_id, cfg.environment, nodes, pods, connectivity, metrics)
    result_json = {
        "site_id": report.site_id,
        "environment": report.environment,
        "timestamp": report.timestamp,
        "nodes": report.nodes,
        "pods": report.pods,
        "connectivity": report.connectivity,
        "metrics": report.metrics,
        "status": report.status,
        "summary": report.summary,
    }

    try:
        send_to_kafka(cfg.kafka_bootstrap_servers, cfg.kafka_health_topic, result_json, logger)
    except Exception as exc:
        push_to_loki(cfg.loki_url, cfg.loki_tenant_id, cfg.loki_labels, [{"level": "error", "event": "kafka_publish_failed", "error": str(exc)}], logger)

    loki_entries = []
    if metrics.get("anomalies"):
        for a in metrics["anomalies"]:
            loki_entries.append({"level": "warn", "event": "anomaly", "details": a, "site_id": cfg.site_id})
    loki_entries.append({"level": "info", "event": "du_health_summary", "summary": report.summary, "status": report.status, "site_id": cfg.site_id})
    try:
        push_to_loki(cfg.loki_url, cfg.loki_tenant_id, cfg.loki_labels, loki_entries, logger)
    except Exception:
        pass

    return result_json


if __name__ == "__main__":
    # Allow local ad-hoc runs for debugging or cron/scheduler usage
    res = run_healthcheck()
    print(json.dumps(res, indent=2))
