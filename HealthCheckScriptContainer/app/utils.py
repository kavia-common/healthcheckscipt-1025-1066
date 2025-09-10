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

from .logging_utils import log_with, setup_logging as setup_structured_logging, trace_id_ctx


def _vault_candidate_urls(vault_addr: str, secret_path: str) -> List[str]:
    """Build candidate URLs for Vault KV v2 and direct path styles."""
    urls = [f"{vault_addr}/v1/{secret_path}"]
    if "/data/" not in secret_path:
        parts = secret_path.split("/", 1)
        if len(parts) == 2:
            urls.append(f"{vault_addr}/v1/{parts[0]}/data/{parts[1]}")
    return urls


def _extract_kubeconfig_from_payload(payload: Dict[str, Any]) -> str:
    """Extract kubeconfig from possible KV v2 payload shapes."""
    if "data" in payload and isinstance(payload["data"], dict):
        inner = payload["data"]
        # KV v2: {data: {data: {...}}}
        if "data" in inner and isinstance(inner["data"], dict):
            return inner["data"].get("kubeconfig", "") or ""
        # KV v1 or direct: {data: {...}}
        return inner.get("kubeconfig", "") or ""
    return ""


def _log_vault_start(logger: logging.Logger, url: str, timeout: float) -> None:
    """Log vault request start with context."""
    log_with(
        logger,
        logging.INFO,
        event="vault_request_start",
        trace_id=trace_id_ctx.get(),
        url=url,
        timeout=timeout,
    )


def _log_vault_end(logger: logging.Logger, status_code: int) -> None:
    """Log vault request end."""
    log_with(
        logger,
        logging.INFO,
        event="vault_request_end",
        status_code=status_code,
        ok=status_code < 400,
    )


def _log_vault_error(logger: logging.Logger, url: str, exc: Exception) -> None:
    """Log vault request error."""
    log_with(
        logger,
        logging.ERROR,
        event="vault_request_error",
        error=str(exc),
        url=url,
    )


# PUBLIC_INTERFACE
def setup_logging(level: str = "INFO") -> logging.Logger:
    """Initialize and return a configured logger (structured JSON)."""
    return setup_structured_logging(level)


# PUBLIC_INTERFACE
def fetch_kubeconfig_from_vault(vault_addr: str, token: str, secret_path: str, timeout: float = 8.0) -> str:
    """Fetch kubeconfig content from HashiCorp Vault KV v2 and return as a string."""
    logger = logging.getLogger("du-healthcheck")
    headers = {"X-Vault-Token": token}
    session = requests.Session()
    session.headers.update(headers)
    urls = _vault_candidate_urls(vault_addr, secret_path)

    last_error: Optional[Exception] = None
    for url in urls:
        try:
            _log_vault_start(logger, url, timeout)
            resp = session.get(url, timeout=timeout)
            _log_vault_end(logger, resp.status_code)
            resp.raise_for_status()
            kube = _extract_kubeconfig_from_payload(resp.json())
            if not kube:
                raise RuntimeError("kubeconfig_empty")
            return kube
        except Exception as exc:
            last_error = exc
            _log_vault_error(logger, url, exc)
            continue
    raise RuntimeError(f"Failed to retrieve kubeconfig from Vault: {last_error}")


# PUBLIC_INTERFACE
def build_k8s_client_from_kubeconfig(kubeconfig_content: str) -> Tuple[client.CoreV1Api, client.AppsV1Api]:
    """Build Kubernetes API clients from kubeconfig file content."""
    logger = logging.getLogger("du-healthcheck")
    tmp_path = "/tmp/kubeconfig_du_health.yaml"
    log_with(logger, logging.INFO, event="k8s_client_init_start", kubeconfig_path=tmp_path)
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(kubeconfig_content)
    try:
        k8s_config.load_kube_config(config_file=tmp_path)
        core_api = client.CoreV1Api()
        apps_api = client.AppsV1Api()
        log_with(logger, logging.INFO, event="k8s_client_init_end", success=True)
        return core_api, apps_api
    except Exception as exc:
        log_with(logger, logging.ERROR, event="k8s_client_init_error", error=str(exc))
        raise


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
    logger = logging.getLogger("du-healthcheck")
    log_with(logger, logging.INFO, event="k8s_list_nodes_start", label_selector=label_selector)
    nodes_info: List[Dict[str, Any]] = []
    try:
        nodes = core_api.list_node(label_selector=label_selector).items
        for n in nodes:
            conditions = {c.type: c.status for c in (n.status.conditions or [])}
            alloc = n.status.allocatable or {}
            entry = {
                "name": n.metadata.name,
                "labels": n.metadata.labels or {},
                "allocatable": {k: str(v) for k, v in alloc.items()},
                "conditions": conditions,
                "ready": conditions.get("Ready") == "True",
            }
            nodes_info.append(entry)
            log_with(logger, logging.DEBUG, event="k8s_node_discovered", node=n.metadata.name, ready=entry["ready"])
        log_with(logger, logging.INFO, event="k8s_list_nodes_end", count=len(nodes_info))
    except ApiException as exc:
        err = str(exc)
        nodes_info.append({"error": err})
        log_with(logger, logging.ERROR, event="k8s_list_nodes_error", error=err)
    return nodes_info


# PUBLIC_INTERFACE
def _container_statuses_for_pod(p) -> List[Dict[str, Any]]:
    statuses = []
    if not p.status.container_statuses:
        return statuses
    for cs in p.status.container_statuses:
        statuses.append(_single_container_status_dict(cs))
    return statuses


def _single_container_status_dict(cs) -> Dict[str, Any]:
    state = "unknown"
    if cs.state.waiting:
        state = f"waiting:{cs.state.waiting.reason}"
    elif cs.state.terminated:
        state = f"terminated:{cs.state.terminated.reason}"
    elif cs.state.running:
        state = "running"
    return {
        "name": cs.name,
        "ready": cs.ready,
        "restarts": cs.restart_count,
        "state": state,
        "image": cs.image,
    }


def _pod_entry_from_obj(p, container_statuses: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "name": p.metadata.name,
        "namespace": p.metadata.namespace,
        "phase": p.status.phase,
        "hostIP": p.status.host_ip,
        "podIP": p.status.pod_ip,
        "labels": p.metadata.labels or {},
        "containers": container_statuses,
    }


def list_pods(core_api: client.CoreV1Api, namespace: str, label_selector: str) -> List[Dict[str, Any]]:
    """List Pods by namespace and label selector with container statuses."""
    logger = logging.getLogger("du-healthcheck")
    log_with(logger, logging.INFO, event="k8s_list_pods_start", namespace=namespace, label_selector=label_selector)
    pods_info: List[Dict[str, Any]] = []
    try:
        pods = core_api.list_namespaced_pod(namespace=namespace, label_selector=label_selector).items
        for p in pods:
            container_statuses = _container_statuses_for_pod(p)
            pod_entry = _pod_entry_from_obj(p, container_statuses)
            pods_info.append(pod_entry)
            log_with(
                logger,
                logging.DEBUG,
                event="k8s_pod_discovered",
                pod=p.metadata.name,
                phase=p.status.phase,
                containers=[c.get("name") for c in container_statuses],
            )
        log_with(logger, logging.INFO, event="k8s_list_pods_end", count=len(pods_info))
    except ApiException as exc:
        err = str(exc)
        pods_info.append({"error": err})
        log_with(logger, logging.ERROR, event="k8s_list_pods_error", error=err)
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
        lower = line.lower()
        _update_metrics_from_line(metrics, line, lower)
    return metrics


def _update_metrics_from_line(metrics: Dict[str, Any], line: str, lower: str) -> None:
    if "srs error" in lower:
        metrics["srs_errors"] += 1
    if "crc" in lower and "ul" in lower and ("fail" in lower or "error" in lower):
        metrics["phy_ul_crc_fail"] += 1
    if "dl mcs avg" in lower:
        try:
            num_str = "".join([c for c in line if (c.isdigit() or c == "." or c == "-")])
            if num_str:
                metrics["phy_dl_mcs_avg"] = float(num_str)
        except Exception:
            pass
    if "rrc connection established" in lower or "rrc: established" in lower:
        metrics["rrc_conn_established"] += 1


# PUBLIC_INTERFACE
def _safe_exec_command(core_api: client.CoreV1Api, namespace: str, pod: str, container: str, command: List[str], timeout_seconds: int = 8) -> Tuple[int, str, str]:
    """Exec command in container; returns (exit_code, stdout, stderr)."""
    full_cmd = ["/bin/sh", "-c", " ".join(command)]
    try:
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


def _container_names_from_pod(pod: Dict[str, Any]) -> List[str]:
    """Extract container names from a pod dict."""
    names: List[str] = []
    for c in pod.get("containers", []):
        if isinstance(c, dict) and "name" in c:
            names.append(c["name"])
    return names


def _collect_single_container_metrics(
    core_api: client.CoreV1Api, namespace: str, pod_name: str, cname: str
) -> Dict[str, Any]:
    """Collect CPU, memory, disk, and SCTP metrics for a single container."""
    cont_res: Dict[str, Any] = {"cpu": None, "memory": None, "disks": None, "sctp": None, "errors": []}
    _collect_cpu_metric(core_api, namespace, pod_name, cname, cont_res)
    _collect_memory_metric(core_api, namespace, pod_name, cname, cont_res)
    _collect_disk_metric(core_api, namespace, pod_name, cname, cont_res)
    _collect_sctp_metric(core_api, namespace, pod_name, cname, cont_res)
    return cont_res


def _collect_cpu_metric(core_api: client.CoreV1Api, namespace: str, pod_name: str, cname: str, cont_res: Dict[str, Any]) -> None:
    """Populate CPU metric for container."""
    code, out, err = _safe_exec_command(core_api, namespace, pod_name, cname, ["top -b -n1 | head -n 5"])
    if code == 0 and out:
        cont_res["cpu"] = {"util_percent": _parse_cpu_from_top(out), "raw": out.splitlines()[:6]}
    else:
        cont_res["errors"].append({"component": "cpu", "error": err})


def _parse_proc_meminfo_to_kv(text: str) -> Dict[str, str]:
    kv: Dict[str, str] = {}
    for ln in text.splitlines():
        if ":" in ln:
            k, v = ln.split(":", 1)
            kv[k.strip()] = v.strip()
    return kv


def _num_from_text(s: str) -> Optional[float]:
    m = re.search(r"([0-9]+)", s or "")
    return float(m.group(1)) if m else None


def _mem_calculate_from_kv(kv: Dict[str, str]) -> Dict[str, Optional[float]]:
    total_kb = _num_from_text(kv.get("MemTotal", "")) or 0.0
    free_kb = _num_from_text(kv.get("MemFree", "")) or 0.0
    buffers_kb = _num_from_text(kv.get("Buffers", "")) or 0.0
    cached_kb = _num_from_text(kv.get("Cached", "")) or 0.0
    available_kb = _num_from_text(kv.get("MemAvailable", "")) or (free_kb + buffers_kb + cached_kb)
    used_kb = max(0.0, total_kb - available_kb)
    total_mb = round(total_kb / 1024.0, 2)
    used_mb = round(used_kb / 1024.0, 2)
    free_mb = round(available_kb / 1024.0, 2)
    used_percent = (used_mb / total_mb) * 100.0 if total_kb > 0 else None
    return {"total_mb": total_mb, "used_mb": used_mb, "free_mb": free_mb, "used_percent": used_percent}


def _collect_memory_metric(core_api: client.CoreV1Api, namespace: str, pod_name: str, cname: str, cont_res: Dict[str, Any]) -> None:
    """Populate memory metric for container."""
    code, out, err = _safe_exec_command(core_api, namespace, pod_name, cname, ["free -m || cat /proc/meminfo | head -n 20"])
    if code != 0 or not out:
        cont_res["errors"].append({"component": "memory", "error": err})
        return
    if "Mem:" in out:
        cont_res["memory"] = _parse_mem_from_free(out)
        return
    try:
        kv = _parse_proc_meminfo_to_kv(out)
        cont_res["memory"] = _mem_calculate_from_kv(kv)
    except Exception as ex:
        cont_res["errors"].append({"component": "memory", "error": str(ex)})


def _collect_disk_metric(core_api: client.CoreV1Api, namespace: str, pod_name: str, cname: str, cont_res: Dict[str, Any]) -> None:
    """Populate disk metric for container."""
    code, out, err = _safe_exec_command(core_api, namespace, pod_name, cname, ["df -h"])
    if code == 0 and out:
        cont_res["disks"] = _parse_df_h(out)
    else:
        cont_res["errors"].append({"component": "disk", "error": err})


def _collect_sctp_metric(core_api: client.CoreV1Api, namespace: str, pod_name: str, cname: str, cont_res: Dict[str, Any]) -> None:
    """Populate SCTP metric for container."""
    code, out, err = _safe_exec_command(core_api, namespace, pod_name, cname, ["ss -H -t -a -p | grep -i sctp || true"])
    if code != 0:
        cont_res["errors"].append({"component": "sctp", "error": err})
        return
    sctp = _parse_sctp_ss(out or "")
    if not sctp["has_sctp"]:
        code2, out2, _ = _safe_exec_command(core_api, namespace, pod_name, cname, ["grep -i sctp /proc/net/protocols || true"])
        if code2 == 0 and out2:
            sctp = {"has_sctp": True, "lines": out2.splitlines()}
    cont_res["sctp"] = sctp


def _collect_container_system_metrics(core_api: client.CoreV1Api, namespace: str, pod: Dict[str, Any]) -> Dict[str, Any]:
    """Collect per-container system metrics for a pod."""
    pod_name = pod.get("name")
    containers = _container_names_from_pod(pod)
    results: Dict[str, Any] = {}
    for cname in containers:
        results[cname] = _collect_single_container_metrics(core_api, namespace, pod_name, cname)
    return results


def collect_logs_and_metrics(core_api: client.CoreV1Api, namespace: str, pods: List[Dict[str, Any]], max_bytes: int = 50000) -> Dict[str, Any]:
    """Fetch recent logs for DU pods and parse metrics, plus per-container system metrics via exec."""
    aggregated: Dict[str, Any] = {"pod_metrics": {}, "anomalies": []}
    for p in pods:
        _process_pod_metrics(core_api, namespace, p, max_bytes, aggregated)
    return aggregated


def _process_pod_metrics(
    core_api: client.CoreV1Api,
    namespace: str,
    pod: Dict[str, Any],
    max_bytes: int,
    aggregated: Dict[str, Any],
) -> None:
    """Process logs and container metrics for a single pod and update aggregated dict."""
    name = pod.get("name")
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
        container_metrics = _collect_container_system_metrics(core_api, namespace, pod)

        aggregated["pod_metrics"][name] = {"radio": radio_metrics, "containers": container_metrics}
        _append_radio_anomalies(name, radio_metrics, aggregated)
        _append_resource_anomalies(name, container_metrics, aggregated)
    except ApiException as exc:
        aggregated["pod_metrics"][name] = {"error": str(exc)}
        aggregated["anomalies"].append({"pod": name, "reason": "log_or_exec_error", "error": str(exc)})


def _append_radio_anomalies(pod_name: str, radio_metrics: Dict[str, Any], aggregated: Dict[str, Any]) -> None:
    """Append radio metric anomalies to aggregated anomalies list if any."""
    if radio_metrics.get("srs_errors", 0) > 0 or radio_metrics.get("phy_ul_crc_fail", 0) > 0:
        aggregated["anomalies"].append({"pod": pod_name, "reason": "radio_metric_anomaly", "metrics": radio_metrics})


def _append_resource_anomalies(pod_name: str, container_metrics: Dict[str, Any], aggregated: Dict[str, Any]) -> None:
    """Append resource pressure anomalies based on CPU/memory thresholds."""
    for cname, cm in container_metrics.items():
        cpu_util = (cm.get("cpu") or {}).get("util_percent")
        mem_used_pct = (cm.get("memory") or {}).get("used_percent")
        if (cpu_util is not None and cpu_util > 90.0) or (mem_used_pct is not None and mem_used_pct > 90.0):
            aggregated["anomalies"].append(
                {
                    "pod": pod_name,
                    "container": cname,
                    "reason": "resource_pressure",
                    "cpu_util_percent": cpu_util,
                    "mem_used_percent": mem_used_pct,
                }
            )


# PUBLIC_INTERFACE
def tcp_connectivity_check(host: Optional[str], port: int, timeout: float) -> Dict[str, Any]:
    """Check TCP connectivity to given host:port."""
    logger = logging.getLogger("du-healthcheck")
    if not host:
        res = {"host": None, "port": port, "reachable": False, "error": "host_not_configured"}
        log_with(logger, logging.WARNING, event="tcp_check_skipped", **res)
        return res
    start = time.time()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    log_with(logger, logging.DEBUG, event="tcp_check_start", host=host, port=port, timeout=timeout)
    try:
        s.connect((host, port))
        s.shutdown(socket.SHUT_RDWR)
        elapsed = time.time() - start
        res = {"host": host, "port": port, "reachable": True, "latency_s": round(elapsed, 4)}
        log_with(logger, logging.INFO, event="tcp_check_end", **res)
        return res
    except Exception as exc:
        res = {"host": host, "port": port, "reachable": False, "error": str(exc)}
        log_with(logger, logging.ERROR, event="tcp_check_error", **res)
        return res
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
    node_ready = _all_nodes_ready(nodes)
    pods_running = _all_pods_running(pods)
    anomalies = metrics.get("anomalies", [])
    status = _derive_status(node_ready, pods_running, anomalies, connectivity)

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


def _all_nodes_ready(nodes: List[Dict[str, Any]]) -> bool:
    """Return True if all nodes are ready, False otherwise."""
    if not nodes:
        return False
    return all(n.get("ready", False) for n in nodes if "ready" in n)


def _all_pods_running(pods: List[Dict[str, Any]]) -> bool:
    """Return True if all pods are in RUNNING phase."""
    pod_states = [p.get("phase", "") for p in pods if "phase" in p]
    if not pod_states:
        return False
    return all(ph.upper() == "RUNNING" for ph in pod_states)


def _derive_status(node_ready: bool, pods_running: bool, anomalies: List[Dict[str, Any]], connectivity: Dict[str, Any]) -> str:
    """Compute overall health status based on conditions."""
    cu_ok = connectivity.get("cu", {}).get("reachable")
    ru_ok = connectivity.get("ru", {}).get("reachable")
    if node_ready and pods_running and not anomalies and cu_ok and ru_ok:
        return "healthy"
    return "degraded"


# PUBLIC_INTERFACE
def send_to_kafka(bootstrap_servers: str, topic: str, payload: Dict[str, Any], logger: logging.Logger) -> None:
    """Send health report to Kafka topic."""
    from .logging_utils import log_with
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": f"du-healthcheck-{os.getenv('HOSTNAME', 'local')}",
        "enable.idempotence": True,
        "compression.type": "zstd",
        "linger.ms": 50,
    }
    log_with(logger, logging.INFO, event="kafka_producer_init", bootstrap_servers=bootstrap_servers, topic=topic)
    producer = Producer(conf)

    def delivery(err, msg):
        if err is not None:
            log_with(logger, logging.ERROR, event="kafka_delivery_failed", error=str(err))
        else:
            log_with(
                logger,
                logging.DEBUG,
                event="kafka_delivery_ok",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    data = json.dumps(payload, default=lambda o: o.__dict__)
    log_with(logger, logging.DEBUG, event="kafka_produce", size_bytes=len(data))
    producer.produce(topic, value=data.encode("utf-8"), callback=delivery)
    producer.flush(5)
    log_with(logger, logging.INFO, event="kafka_flush_done")


# PUBLIC_INTERFACE
def push_to_loki(loki_url: str, tenant_id: Optional[str], labels: str, entries: List[Dict[str, Any]], logger: logging.Logger) -> None:
    """Push logs/entries to Loki via HTTP API."""
    from .logging_utils import log_with
    def parse_labels(labels_str: str) -> Dict[str, str]:
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
    url = loki_url.rstrip("/") + "/loki/api/v1/push"
    log_with(logger, logging.INFO, event="loki_push_start", url=url, entries=len(entries))
    try:
        resp = requests.post(url, data=json.dumps(payload), headers=headers, timeout=5)
        if resp.status_code >= 300:
            log_with(logger, logging.ERROR, event="loki_push_failed", status_code=resp.status_code, response=resp.text[:500])
        else:
            log_with(logger, logging.INFO, event="loki_push_ok", status_code=resp.status_code)
    except Exception as exc:
        log_with(logger, logging.ERROR, event="loki_push_error", error=str(exc))
