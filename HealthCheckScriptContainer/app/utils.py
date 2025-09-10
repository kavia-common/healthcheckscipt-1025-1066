import json
import logging
import os
import socket
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from kubernetes import client, config as k8s_config
from kubernetes.client import ApiException
from confluent_kafka import Producer


# PUBLIC_INTERFACE
def setup_logging(level: str = "INFO") -> logging.Logger:
    """Initialize and return a configured logger based on provided log level."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    logger = logging.getLogger("du-healthcheck")
    logger.setLevel(log_level)
    return logger


# PUBLIC_INTERFACE
def fetch_kubeconfig_from_vault(vault_addr: str, token: str, secret_path: str, timeout: float = 8.0) -> str:
    """Fetch kubeconfig content from HashiCorp Vault KV v2 and return as a string."""
    # Detect if path is kv-v2 and build URL format: /v1/<mount>/data/<path>
    # Allow user to pass full path; we try both direct and kv v2 path form.
    headers = {"X-Vault-Token": token}
    session = requests.Session()
    session.headers.update(headers)
    urls = [
        f"{vault_addr}/v1/{secret_path}",
    ]

    # Attempt to construct kv-v2 path automatically if not already a /data/ URL
    if "/data/" not in secret_path:
        parts = secret_path.split("/", 1)
        if len(parts) == 2:
            urls.append(f"{vault_addr}/v1/{parts[0]}/data/{parts[1]}")

    last_error: Optional[Exception] = None
    for url in urls:
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            payload = resp.json()
            # KV v2 puts secret at data.data, KV v1 would be at data directly
            if "data" in payload and isinstance(payload["data"], dict) and "data" in payload["data"]:
                return payload["data"]["data"].get("kubeconfig", "")
            if "data" in payload and isinstance(payload["data"], dict):
                return payload["data"].get("kubeconfig", "")
        except Exception as exc:
            last_error = exc
            continue
    raise RuntimeError(f"Failed to retrieve kubeconfig from Vault: {last_error}")


# PUBLIC_INTERFACE
def build_k8s_client_from_kubeconfig(kubeconfig_content: str) -> Tuple[client.CoreV1Api, client.AppsV1Api]:
    """Build Kubernetes API clients from kubeconfig file content."""
    # Write to a temp file (in container)
    tmp_path = "/tmp/kubeconfig_du_health.yaml"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(kubeconfig_content)
    k8s_config.load_kube_config(config_file=tmp_path)
    core_api = client.CoreV1Api()
    apps_api = client.AppsV1Api()
    return core_api, apps_api


@dataclass
class HealthResult:
    site_id: Optional[str]
    environment: str
    timestamp: float
    nodes: List[Dict[str, Any]]
    pods: List[Dict[str, Any]]
    connectivity: Dict[str, Any]
    metrics: Dict[str, Any]
    status: str
    summary: Dict[str, Any]


# PUBLIC_INTERFACE
def list_nodes(core_api: client.CoreV1Api, label_selector: str) -> List[Dict[str, Any]]:
    """List Kubernetes nodes by label selector and basic readiness."""
    nodes_info: List[Dict[str, Any]] = []
    try:
        nodes = core_api.list_node(label_selector=label_selector).items
        for n in nodes:
            conditions = {c.type: c.status for c in (n.status.conditions or [])}
            alloc = n.status.allocatable or {}
            nodes_info.append(
                {
                    "name": n.metadata.name,
                    "labels": n.metadata.labels or {},
                    "allocatable": {k: str(v) for k, v in alloc.items()},
                    "conditions": conditions,
                    "ready": conditions.get("Ready") == "True",
                }
            )
    except ApiException as exc:
        nodes_info.append({"error": str(exc)})
    return nodes_info


# PUBLIC_INTERFACE
def list_pods(core_api: client.CoreV1Api, namespace: str, label_selector: str) -> List[Dict[str, Any]]:
    """List Pods by namespace and label selector with container statuses."""
    pods_info: List[Dict[str, Any]] = []
    try:
        pods = core_api.list_namespaced_pod(namespace=namespace, label_selector=label_selector).items
        for p in pods:
            container_statuses = []
            if p.status.container_statuses:
                for cs in p.status.container_statuses:
                    restarts = cs.restart_count
                    state = "unknown"
                    if cs.state.waiting:
                        state = f"waiting:{cs.state.waiting.reason}"
                    elif cs.state.terminated:
                        state = f"terminated:{cs.state.terminated.reason}"
                    elif cs.state.running:
                        state = "running"
                    container_statuses.append(
                        {
                            "name": cs.name,
                            "ready": cs.ready,
                            "restarts": restarts,
                            "state": state,
                            "image": cs.image,
                        }
                    )
            pods_info.append(
                {
                    "name": p.metadata.name,
                    "namespace": p.metadata.namespace,
                    "phase": p.status.phase,
                    "hostIP": p.status.host_ip,
                    "podIP": p.status.pod_ip,
                    "labels": p.metadata.labels or {},
                    "containers": container_statuses,
                }
            )
    except ApiException as exc:
        pods_info.append({"error": str(exc)})
    return pods_info


def _parse_metrics_from_text(log_text: str) -> Dict[str, Any]:
    """Simple parser for radio access metrics from logs."""
    metrics: Dict[str, Any] = {
        "srs_errors": 0,
        "phy_ul_crc_fail": 0,
        "phy_dl_mcs_avg": None,
        "rrc_conn_established": 0,
    }
    for line in log_text.splitlines():
        l = line.lower()
        if "srs error" in l:
            metrics["srs_errors"] += 1
        if "crc" in l and "ul" in l and ("fail" in l or "error" in l):
            metrics["phy_ul_crc_fail"] += 1
        if "dl mcs avg" in l:
            try:
                # grab number at end of line
                num_str = "".join([c for c in line if (c.isdigit() or c == "." or c == "-")])
                if num_str:
                    metrics["phy_dl_mcs_avg"] = float(num_str)
            except Exception:
                pass
        if "rrc connection established" in l or "rrc: established" in l:
            metrics["rrc_conn_established"] += 1
    return metrics


# PUBLIC_INTERFACE
def collect_logs_and_metrics(core_api: client.CoreV1Api, namespace: str, pods: List[Dict[str, Any]], max_bytes: int = 50000) -> Dict[str, Any]:
    """Fetch recent logs for DU pods and parse metrics."""
    aggregated: Dict[str, Any] = {"pod_metrics": {}, "anomalies": []}
    for p in pods:
        name = p.get("name")
        try:
            log_text = core_api.read_namespaced_pod_log(
                name=name,
                namespace=namespace,
                tail_lines=500,
                timestamps=True,
                _preload_content=True,
            )
            if len(log_text) > max_bytes:
                log_text = log_text[-max_bytes:]
            metrics = _parse_metrics_from_text(log_text)
            aggregated["pod_metrics"][name] = metrics
            # simple anomaly heuristics
            if metrics.get("srs_errors", 0) > 0 or metrics.get("phy_ul_crc_fail", 0) > 0:
                aggregated["anomalies"].append(
                    {
                        "pod": name,
                        "reason": "radio_metric_anomaly",
                        "metrics": metrics,
                    }
                )
        except ApiException as exc:
            aggregated["pod_metrics"][name] = {"error": str(exc)}
            aggregated["anomalies"].append({"pod": name, "reason": "log_fetch_error", "error": str(exc)})
    return aggregated


# PUBLIC_INTERFACE
def tcp_connectivity_check(host: Optional[str], port: int, timeout: float) -> Dict[str, Any]:
    """Check TCP connectivity to given host:port."""
    if not host:
        return {"host": None, "port": port, "reachable": False, "error": "host_not_configured"}
    start = time.time()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.shutdown(socket.SHUT_RDWR)
        elapsed = time.time() - start
        return {"host": host, "port": port, "reachable": True, "latency_s": round(elapsed, 4)}
    except Exception as exc:
        return {"host": host, "port": port, "reachable": False, "error": str(exc)}
    finally:
        s.close()


# PUBLIC_INTERFACE
def build_health_report(
    site_id: Optional[str],
    environment: str,
    nodes: List[Dict[str, Any]],
    pods: List[Dict[str, Any]],
    connectivity: Dict[str, Any],
    metrics: Dict[str, Any],
) -> HealthResult:
    """Assemble a structured health report."""
    timestamp = time.time()
    node_ready = all([n.get("ready", False) for n in nodes if "ready" in n]) if nodes else False
    pod_states = [p.get("phase", "") for p in pods if "phase" in p]
    pods_running = all([ph.upper() == "RUNNING" for ph in pod_states]) if pod_states else False
    anomalies = metrics.get("anomalies", [])
    status = "healthy" if node_ready and pods_running and not anomalies and connectivity.get("cu", {}).get("reachable") and connectivity.get("ru", {}).get("reachable") else "degraded"

    summary = {
        "node_ready_all": node_ready,
        "pods_running_all": pods_running,
        "anomaly_count": len(anomalies),
        "cu_reachable": connectivity.get("cu", {}).get("reachable", False),
        "ru_reachable": connectivity.get("ru", {}).get("reachable", False),
    }
    return HealthResult(
        site_id=site_id,
        environment=environment,
        timestamp=timestamp,
        nodes=nodes,
        pods=pods,
        connectivity=connectivity,
        metrics=metrics,
        status=status,
        summary=summary,
    )


# PUBLIC_INTERFACE
def send_to_kafka(bootstrap_servers: str, topic: str, payload: Dict[str, Any], logger: logging.Logger) -> None:
    """Send health report to Kafka topic."""
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": f"du-healthcheck-{os.getenv('HOSTNAME', 'local')}",
        "enable.idempotence": True,
        "compression.type": "zstd",
        "linger.ms": 50,
    }
    producer = Producer(conf)

    def delivery(err, msg):
        if err is not None:
            logger.error(f"Kafka delivery failed: {err}")
        else:
            logger.debug(f"Kafka delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

    data = json.dumps(payload, default=lambda o: o.__dict__)
    producer.produce(topic, value=data.encode("utf-8"), callback=delivery)
    producer.flush(5)


# PUBLIC_INTERFACE
def push_to_loki(loki_url: str, tenant_id: Optional[str], labels: str, entries: List[Dict[str, Any]], logger: logging.Logger) -> None:
    """Push logs/entries to Loki via HTTP API."""
    # Loki expected payload
    # {"streams": [{"stream": {"label": "value"}, "values": [["<ns>", "<log line>"], ...]}]}
    # labels must be as prom labels string, convert to dict as stream labels
    def parse_labels(labels_str: str) -> Dict[str, str]:
        # very small parser for format: {k="v",k2="v2"}
        res = {}
        s = labels_str.strip()
        if not (s.startswith("{") and s.endswith("}")):
            return res
        inner = s[1:-1].strip()
        if not inner:
            return res
        parts = [p.strip() for p in inner.split(",")]
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                res[k.strip()] = v.strip().strip('"')
        return res

    stream_labels = parse_labels(labels)
    ns_entries = []
    for e in entries:
        ts_ns = str(int(time.time() * 1e9))
        ns_entries.append([ts_ns, json.dumps(e)])

    payload = {"streams": [{"stream": stream_labels, "values": ns_entries}]}
    headers = {"Content-Type": "application/json"}
    if tenant_id:
        headers["X-Scope-OrgID"] = tenant_id
    try:
        resp = requests.post(loki_url.rstrip("/") + "/loki/api/v1/push", data=json.dumps(payload), headers=headers, timeout=5)
        if resp.status_code >= 300:
            logger.error(f"Loki push failed: {resp.status_code} {resp.text}")
        else:
            logger.debug("Pushed logs to Loki successfully.")
    except Exception as exc:
        logger.error(f"Loki push error: {exc}")
