from flask_smorest import Blueprint
from flask.views import MethodView
from flask import request, jsonify
from marshmallow import Schema, fields
from typing import Any, Dict

from ..config import AppConfig
from ..utils import (
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


class TriggerSchema(Schema):
    site_id = fields.String(required=False, allow_none=True, description="Optional site ID to include in report")
    environment = fields.String(required=False, description="Environment override: dev|stage|prod")
    node_label_selector = fields.String(required=False, description="K8s node label selector")
    pod_label_selector = fields.String(required=False, description="K8s pod label selector")
    namespace = fields.String(required=False, description="K8s namespace")
    airflow_run_id = fields.String(required=False, description="Airflow run identifier for traceability")


blp = Blueprint(
    "System",
    "system",
    url_prefix="/system",
    description="System integration endpoints to trigger DU health checks",
)


@blp.route("/healthcheck")
class TriggerHealthCheck(MethodView):
    """Trigger the DU health check process via HTTP POST."""
    # PUBLIC_INTERFACE
    def post(self):
        """
        Trigger a DU health check execution.

        Request JSON:
          - site_id: optional site identifier
          - environment: optional environment override (dev|stage|prod)
          - node_label_selector: optional K8s node label selector
          - pod_label_selector: optional K8s pod label selector
          - namespace: K8s namespace for DU pods
          - airflow_run_id: optional Airflow run id for trace

        Returns:
          JSON health report with status and summary. Also publishes to Kafka and pushes anomalies to Loki.
        """
        data: Dict[str, Any] = request.get_json(silent=True) or {}
        schema = TriggerSchema()
        errors = schema.validate(data)
        if errors:
            return {"error": errors}, 400

        cfg = AppConfig()
        if "environment" in data and data["environment"]:
            cfg.environment = data["environment"]
        if "node_label_selector" in data and data["node_label_selector"]:
            cfg.node_label_selector = data["node_label_selector"]
        if "pod_label_selector" in data and data["pod_label_selector"]:
            cfg.pod_label_selector = data["pod_label_selector"]
        if "namespace" in data and data["namespace"]:
            cfg.namespace = data["namespace"]
        if "site_id" in data:
            cfg.site_id = data.get("site_id")

        logger = setup_logging(cfg.log_level)

        try:
            cfg.validate()
        except Exception as exc:
            return {"error": f"Configuration error: {exc}"}, 500

        # Retrieve kubeconfig from Vault
        try:
            kubeconfig = fetch_kubeconfig_from_vault(cfg.vault_addr, cfg.vault_token, cfg.vault_kubeconfig_path, timeout=cfg.request_timeout)
        except Exception as exc:
            logger.exception("Failed to fetch kubeconfig from Vault")
            push_to_loki(cfg.loki_url, cfg.loki_tenant_id, cfg.loki_labels, [{"level": "error", "event": "vault_kubeconfig_fetch_failed", "error": str(exc)}], logger)
            return {"error": f"Vault kubeconfig fetch failed: {exc}"}, 500

        # Build K8s clients
        try:
            core_api, _ = build_k8s_client_from_kubeconfig(kubeconfig)
        except Exception as exc:
            logger.exception("Failed to build Kubernetes client")
            push_to_loki(cfg.loki_url, cfg.loki_tenant_id, cfg.loki_labels, [{"level": "error", "event": "k8s_client_build_failed", "error": str(exc)}], logger)
            return {"error": f"Kubernetes client init failed: {exc}"}, 500

        # Collect data
        nodes = list_nodes(core_api, cfg.node_label_selector)
        pods = list_pods(core_api, cfg.namespace, cfg.pod_label_selector)
        metrics = collect_logs_and_metrics(core_api, cfg.namespace, pods)

        # Connectivity
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

        # Publish to Kafka
        try:
            send_to_kafka(cfg.kafka_bootstrap_servers, cfg.kafka_health_topic, result_json, logger)
        except Exception as exc:
            logger.exception("Kafka publish failed")
            push_to_loki(cfg.loki_url, cfg.loki_tenant_id, cfg.loki_labels, [{"level": "error", "event": "kafka_publish_failed", "error": str(exc)}], logger)

        # Push anomalies and failures to Loki
        loki_entries = []
        if metrics.get("anomalies"):
            for a in metrics["anomalies"]:
                loki_entries.append({"level": "warn", "event": "anomaly", "details": a, "site_id": cfg.site_id})
        # Add overall summary at info
        loki_entries.append({"level": "info", "event": "du_health_summary", "summary": report.summary, "status": report.status, "site_id": cfg.site_id})
        try:
            push_to_loki(cfg.loki_url, cfg.loki_tenant_id, cfg.loki_labels, loki_entries, logger)
        except Exception:
            # already logged inside push_to_loki
            pass

        return jsonify(result_json), 200
