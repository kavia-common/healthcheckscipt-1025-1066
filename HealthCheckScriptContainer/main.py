#!/usr/bin/env python3
"""
DU Health Check - Standalone CLI

This script performs health checks on DU sites in a 5G Kubernetes environment.
It:
- Accepts site ID and environment via CLI arguments or environment variables
- Retrieves kubeconfig securely from Vault
- Filters K8s nodes/pods by labels
- Parses DU logs for radio access metrics
- Checks TCP connectivity to CU and RU
- Generates reports and publishes to Kafka
- Pushes anomalies/logs to Loki
- Provides console logging suitable for Airflow triggering

Usage:
  python main.py --site-id <SITE_ID> --environment <ENV> --namespace du-ns \
      --node-label-selector 'ran.site=true,site_id=site-001' \
      --pod-label-selector 'du=true' --log-level INFO

Environment variables (see .env.example) can also be used for configuration.
"""

import argparse
import json
import sys
from typing import Any, Dict, Optional

from app.config import AppConfig
from app.utils import (
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="du-healthcheck",
        description="Run DU health checks against a Kubernetes environment."
    )
    parser.add_argument("--site-id", dest="site_id", type=str, default=None, help="Optional site identifier to include in the report")
    # Back-compat: keep --environment and add --env (alias)
    parser.add_argument("--environment", dest="environment", type=str, default=None, help="Environment override: dev|stage|prod")
    parser.add_argument("--env", dest="env", type=str, default=None, help="Environment alias: dev|stage|prod (same as --environment)")
    parser.add_argument("--namespace", dest="namespace", type=str, default=None, help="K8s namespace")
    parser.add_argument("--node-label-selector", dest="node_label_selector", type=str, default=None, help="K8s node label selector")
    parser.add_argument("--pod-label-selector", dest="pod_label_selector", type=str, default=None, help="K8s pod label selector")
    parser.add_argument("--log-level", dest="log_level", type=str, default=None, help="Logging level (DEBUG, INFO, WARN, ERROR)")
    parser.add_argument("--no-kafka", dest="no_kafka", action="store_true", help="Do not publish results to Kafka")
    parser.add_argument("--no-loki", dest="no_loki", action="store_true", help="Do not push logs to Loki")
    return parser.parse_args()


# PUBLIC_INTERFACE
def run_cli(site_id: Optional[str] = None, environment: Optional[str] = None, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Run the DU health check workflow as a function suitable for Airflow or direct import.

    Args:
        site_id: Optional site identifier to include in the report (overrides env).
        environment: Optional environment override (dev|stage|prod).
        overrides: Optional dict of config field overrides (e.g., namespace, selectors).

    Returns:
        Dict[str, Any]: Structured health report to be printed or returned to schedulers.
    """
    cfg = AppConfig()
    # Load from YAML + env using target environment
    cfg.load_from_files_and_env(env_override=environment)

    # Apply direct overrides and explicit args after load (highest precedence)
    if environment:
        cfg.environment = environment
    if site_id is not None:
        cfg.site_id = site_id
    if overrides:
        for k, v in overrides.items():
            if hasattr(cfg, k) and v is not None:
                setattr(cfg, k, v)

    logger = setup_logging(cfg.log_level)

    # Validate configuration
    cfg.validate()
    logger.info("Starting DU health check")
    logger.debug(f"Effective configuration: environment={cfg.environment}, site_id={cfg.site_id}, namespace={cfg.namespace}")

    # Retrieve kubeconfig from Vault
    kubeconfig = fetch_kubeconfig_from_vault(
        cfg.vault_addr, cfg.vault_token, cfg.vault_kubeconfig_path, timeout=cfg.request_timeout
    )
    logger.info("Fetched kubeconfig from Vault")

    # Build K8s clients
    core_api, _ = build_k8s_client_from_kubeconfig(kubeconfig)
    logger.info("Initialized Kubernetes client")

    # Collect cluster data
    nodes = list_nodes(core_api, cfg.node_label_selector)
    logger.info(f"Discovered nodes: {len(nodes)}")
    pods = list_pods(core_api, cfg.namespace, cfg.pod_label_selector)
    logger.info(f"Discovered pods: {len(pods)}")

    metrics = collect_logs_and_metrics(core_api, cfg.namespace, pods)
    logger.info("Collected DU pod logs and parsed metrics")

    # Connectivity checks
    connectivity = {
        "cu": tcp_connectivity_check(cfg.cu_host, cfg.cu_port, cfg.connectivity_timeout),
        "ru": tcp_connectivity_check(cfg.ru_host, cfg.ru_port, cfg.connectivity_timeout),
    }
    logger.info(f"Connectivity - CU: {connectivity.get('cu')}, RU: {connectivity.get('ru')}")

    # Build report
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

    return result_json


def main() -> int:
    args = _parse_args()
    # select environment from --env or --environment (priority to --env)
    env_arg = args.env if getattr(args, "env", None) else args.environment

    # Compose overrides from CLI args
    overrides: Dict[str, Any] = {}
    if args.namespace:
        overrides["namespace"] = args.namespace
    if args.node_label_selector:
        overrides["node_label_selector"] = args.node_label_selector
    if args.pod_label_selector:
        overrides["pod_label_selector"] = args.pod_label_selector
    if args.log_level:
        overrides["log_level"] = args.log_level

    try:
        result = run_cli(site_id=args.site_id, environment=env_arg, overrides=overrides)
    except Exception as exc:
        print(f"[FATAL] DU health check failed: {exc}", file=sys.stderr)
        return 2

    # After success, conditionally export to Kafka/Loki using the same loaded config precedence
    cfg = AppConfig()
    cfg.load_from_files_and_env(env_override=env_arg)
    if args.site_id is not None:
        cfg.site_id = args.site_id
    for k, v in overrides.items():
        setattr(cfg, k, v)

    logger = setup_logging(cfg.log_level)

    # Publish to Kafka unless disabled
    if not args.no_kafka:
        try:
            send_to_kafka(cfg.kafka_bootstrap_servers, cfg.kafka_health_topic, result, logger)
            logger.info("Published health report to Kafka")
        except Exception as exc:
            logger.exception("Kafka publish failed")
            if not args.no_loki:
                try:
                    push_to_loki(cfg.loki_url, cfg.loki_tenant_id, cfg.loki_labels,
                                 [{"level": "error", "event": "kafka_publish_failed", "error": str(exc)}], logger)
                except Exception:
                    pass

    # Push anomalies and summary to Loki unless disabled
    if not args.no_loki:
        loki_entries = []
        metrics = result.get("metrics", {})
        if isinstance(metrics, dict) and metrics.get("anomalies"):
            for a in metrics["anomalies"]:
                loki_entries.append({"level": "warn", "event": "anomaly", "details": a, "site_id": cfg.site_id})
        loki_entries.append({"level": "info", "event": "du_health_summary", "summary": result.get("summary"), "status": result.get("status"), "site_id": cfg.site_id})
        try:
            push_to_loki(cfg.loki_url, cfg.loki_tenant_id, cfg.loki_labels, loki_entries, logger)
            logger.info("Pushed logs to Loki")
        except Exception:
            pass

    print(json.dumps(result, indent=2))
    return 0 if result.get("status") == "healthy" else 1


if __name__ == "__main__":
    sys.exit(main())
