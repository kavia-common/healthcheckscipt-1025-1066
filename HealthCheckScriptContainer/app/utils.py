import json
import logging
import os
import re
import socket
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from kubernetes import client, config as k8s_config
from kubernetes.client import ApiException
from kubernetes.stream import stream
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
def _safe_exec_command(core_api: client.CoreV1Api, namespace: str, pod: str, container: str, command: List[str], timeout_seconds: int = 8) -> Tuple[int, str, str]:
    """
    Execute a command in a container using Kubernetes exec and return (exit_code, stdout, stderr).

    Commands are executed with /bin/sh -c to maximize compatibility across distros.
    """
    # Use /bin/sh -c "user command" for portability
    full_cmd = ["/bin/sh", "-c", " ".join(command)]
    try:
        # stream returns the command output; capture stderr by enabling _preload_content=False if needed
        resp = stream(
            core_api.connect_get_namespaced_pod_exec,
            pod,
            namespace,
            container=container,
            command=full_cmd,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _request_timeout=timeout_seconds,
        )
        # When preload is True (default here), resp is stdout text, and Kubernetes does not expose exit code directly.
        # We heuristically set exit_code=0 if no stderr indicator. For robustness re-run with 'sh -c "<cmd>; echo $? >&2"'
        # However that complicates parsing; we will consider non-empty output as success.
        return 0, resp or "", ""
    except Exception as exc:
        return 1, "", str(exc)


def _parse_cpu_from_top(output: str) -> Optional[float]:
    """
    Parse CPU utilization percentage from 'top -b -n1' output (Linux).
    Returns total CPU utilization (user+system+nice+irq+softirq+steal) if parsable.
    """
    # Common formats:
    # %Cpu(s):  1.7 us,  0.5 sy,  0.0 ni, 97.6 id,  0.1 wa,  0.0 hi,  0.1 si,  0.0 st
    # Cpu(s):  3.0%us,  1.0%sy,  0.0%ni, 95.5%id,  0.5%wa,  0.0%hi,  0.0%si,  0.0%st
    m = re.search(r"Cpu\(s\):\s*([^\\n]+)", output) or re.search(r"%Cpu\(s\):\s*([^\\n]+)", output)
    if not m:
        return None
    seg = m.group(1)
    # Extract all number+label pairs
    # Prefer computing utilization as 100 - idle if available; otherwise fallback to sum(us+sy)
    idm = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%?id", seg)
    if idm:
        try:
            idle = float(idm.group(1))
            return max(0.0, min(100.0, 100.0 - idle))
        except Exception:
            return None
    # Fallback: try summation of us+sy
    usm = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%?us", seg)
    sym = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%?sy", seg)
    if usm and sym:
        try:
            return float(usm.group(1)) + float(sym.group(1))
        except Exception:
            return None
    return None


def _parse_mem_from_free(output: str) -> Dict[str, Optional[float]]:
    """
    Parse memory usage from 'free -m' output.
    Returns a dict with total_mb, used_mb, free_mb, used_percent.
    """
    # Example:
    #               total        used        free      shared  buff/cache   available
    # Mem:           7957        1400        5216         123        1340        6150
    lines = output.strip().splitlines()
    for ln in lines:
        if ln.lower().startswith("mem:"):
            parts = [p for p in ln.split() if p]
            if len(parts) >= 7:
                try:
                    total = float(parts[1])
                    used = float(parts[2])
                    free = float(parts[3])
                    used_percent = (used / total) * 100.0 if total > 0 else None
                    return {
                        "total_mb": total,
                        "used_mb": used,
                        "free_mb": free,
                        "used_percent": used_percent,
                    }
                except Exception:
                    return {"total_mb": None, "used_mb": None, "free_mb": None, "used_percent": None}
    return {"total_mb": None, "used_mb": None, "free_mb": None, "used_percent": None}


def _parse_df_h(output: str) -> List[Dict[str, Any]]:
    """
    Parse disk utilization from 'df -h' output.
    Returns a list of mount usage dicts containing filesystem, size, used, avail, use_percent, mountpoint.
    """
    lines = [l for l in output.strip().splitlines() if l.strip()]
    if not lines:
        return []
    # Skip header line
    entries = []
    for ln in lines[1:]:
        parts = [p for p in ln.split() if p]
        if len(parts) >= 6:
            fs, size, used, avail, usep, mnt = parts[:6]
            entries.append(
                {
                    "filesystem": fs,
                    "size": size,
                    "used": used,
                    "available": avail,
                    "use_percent": usep,
                    "mountpoint": mnt,
                }
            )
    return entries


def _parse_sctp_ss(output: str) -> Dict[str, Any]:
    """
    Parse SCTP status from 'ss -H -t -a -p | grep sctp' or equivalent.
    Returns dict with has_sctp (bool), lines (list of matching lines).
    """
    lines = [l for l in output.splitlines() if l.strip()]
    has_sctp = any(("sctp" in l.lower()) for l in lines)
    return {"has_sctp": has_sctp, "lines": lines}


def _collect_container_system_metrics(core_api: client.CoreV1Api, namespace: str, pod: Dict[str, Any]) -> Dict[str, Any]:
    """
    For each applicable container in the given pod, exec into the container and collect:
      - CPU utilization (via 'top -b -n1' where available)
      - RAM usage (via 'free -m' or fall back to /proc/meminfo)
      - Disk utilization (via 'df -h')
      - SCTP status (via 'ss -H -t -a -p | grep sctp' or similar)

    Returns:
        Dict[str, Any]: keyed by container name with metrics and raw outputs for traceability.
    """
    pod_name = pod.get("name")
    containers = []
    for c in pod.get("containers", []):
        if isinstance(c, dict) and "name" in c:
            containers.append(c["name"])
    results: Dict[str, Any] = {}

    for cname in containers:
        cont_res: Dict[str, Any] = {"cpu": None, "memory": None, "disks": None, "sctp": None, "errors": []}
        # CPU utilization
        # Command: Use 'top -b -n1' to retrieve CPU summary; parse %id to compute utilization.
        code, out, err = _safe_exec_command(core_api, namespace, pod_name, cname, ["top -b -n1 | head -n 5"])
        if code == 0 and out:
            cont_res["cpu"] = {"util_percent": _parse_cpu_from_top(out), "raw": out.splitlines()[:6]}
        else:
            cont_res["errors"].append({"component": "cpu", "error": err})

        # Memory usage
        # Command: 'free -m' for totals; fallback to /proc/meminfo if free is missing.
        code, out, err = _safe_exec_command(core_api, namespace, pod_name, cname, ["free -m || cat /proc/meminfo | head -n 20"])
        if code == 0 and out:
            if "Mem:" in out:
                cont_res["memory"] = _parse_mem_from_free(out)
            else:
                # rudimentary parse from meminfo (in kB)
                try:
                    kv = {}
                    for ln in out.splitlines():
                        if ":" in ln:
                            k, v = ln.split(":", 1)
                            kv[k.strip()] = v.strip()
                    def _num_from(s: str) -> Optional[float]:
                        m = re.search(r"([0-9]+)", s or "")
                        return float(m.group(1)) if m else None
                    total_kb = _num_from(kv.get("MemTotal", "")) or 0.0
                    free_kb = _num_from(kv.get("MemFree", "")) or 0.0
                    buffers_kb = _num_from(kv.get("Buffers", "")) or 0.0
                    cached_kb = _num_from(kv.get("Cached", "")) or 0.0
                    available_kb = _num_from(kv.get("MemAvailable", "")) or (free_kb + buffers_kb + cached_kb)
                    used_kb = max(0.0, total_kb - available_kb)
                    cont_res["memory"] = {
                        "total_mb": round(total_kb / 1024.0, 2),
                        "used_mb": round(used_kb / 1024.0, 2),
                        "free_mb": round(available_kb / 1024.0, 2),
                        "used_percent": (round(used_kb / 1024.0, 2) / round(total_kb / 1024.0, 2)) * 100.0
                        if total_kb > 0
                        else None,
                    }
                except Exception as ex:
                    cont_res["errors"].append({"component": "memory", "error": str(ex)})
        else:
            cont_res["errors"].append({"component": "memory", "error": err})

        # Disk utilization
        # Command: 'df -h' to get filesystem usage inside container filesystem namespace.
        code, out, err = _safe_exec_command(core_api, namespace, pod_name, cname, ["df -h"])
        if code == 0 and out:
            cont_res["disks"] = _parse_df_h(out)
        else:
            cont_res["errors"].append({"component": "disk", "error": err})

        # SCTP status
        # Command: 'ss -H -t -a -p | grep -i sctp' to check SCTP sockets (if ss present).
        # Fallback: 'cat /proc/net/sctp/*' may exist on kernels with SCTP support.
        code, out, err = _safe_exec_command(core_api, namespace, pod_name, cname, ["ss -H -t -a -p | grep -i sctp || true"])
        if code == 0:
            sctp = _parse_sctp_ss(out or "")
            # If empty and ss missing, attempt /proc approach
            if not sctp["has_sctp"]:
                code2, out2, err2 = _safe_exec_command(core_api, namespace, pod_name, cname, ["grep -i sctp /proc/net/protocols || true"])
                if code2 == 0 and out2:
                    sctp = {"has_sctp": True, "lines": out2.splitlines()}
            cont_res["sctp"] = sctp
        else:
            cont_res["errors"].append({"component": "sctp", "error": err})

        results[cname] = cont_res

    return results


def collect_logs_and_metrics(core_api: client.CoreV1Api, namespace: str, pods: List[Dict[str, Any]], max_bytes: int = 50000) -> Dict[str, Any]:
    """Fetch recent logs for DU pods and parse metrics, plus per-container system metrics via exec.

    For each applicable container:
      - Run 'top -b -n1' to estimate CPU utilization (parsed as 100 - idle)
      - Run 'free -m' (or parse /proc/meminfo) for memory usage
      - Run 'df -h' to capture disk usage per mountpoint
      - Run 'ss -H -t -a -p | grep -i sctp' (fallback to /proc) to detect SCTP

    Returns:
      {
        "pod_metrics": { "<pod>": { "radio": {...}, "containers": { "<container>": {...} } } },
        "anomalies": [ ... ]
      }
    """
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
            radio_metrics = _parse_metrics_from_text(log_text)

            # Exec into containers to fetch system metrics
            container_metrics = _collect_container_system_metrics(core_api, namespace, p)

            aggregated["pod_metrics"][name] = {
                "radio": radio_metrics,
                "containers": container_metrics,
            }

            # simple anomaly heuristics based on radio metrics and container resource red flags
            if radio_metrics.get("srs_errors", 0) > 0 or radio_metrics.get("phy_ul_crc_fail", 0) > 0:
                aggregated["anomalies"].append(
                    {
                        "pod": name,
                        "reason": "radio_metric_anomaly",
                        "metrics": radio_metrics,
                    }
                )

            # Add anomalies if any container shows very high CPU or memory use
            for cname, cm in container_metrics.items():
                cpu_util = (cm.get("cpu") or {}).get("util_percent")
                mem_used_pct = (cm.get("memory") or {}).get("used_percent")
                if (cpu_util is not None and cpu_util > 90.0) or (mem_used_pct is not None and mem_used_pct > 90.0):
                    aggregated["anomalies"].append(
                        {
                            "pod": name,
                            "container": cname,
                            "reason": "resource_pressure",
                            "cpu_util_percent": cpu_util,
                            "mem_used_percent": mem_used_pct,
                        }
                    )

        except ApiException as exc:
            aggregated["pod_metrics"][name] = {"error": str(exc)}
            aggregated["anomalies"].append({"pod": name, "reason": "log_or_exec_error", "error": str(exc)})
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
