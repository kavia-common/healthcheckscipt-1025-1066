import re
from typing import Any, Dict, List, Optional

from kubernetes import client

from .kube_utils import _safe_exec_command


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


def _parse_cpu_from_top(output: str) -> Optional[float]:
    """
    Parse CPU utilization percentage from 'top -b -n1' output (Linux).
    Returns total CPU utilization (user+system+nice+irq+softirq+steal) if parsable.
    """
    m = re.search(r"Cpu\\(s\\):\\s*([^\\n]+)", output) or re.search(r"%Cpu\\(s\\):\\s*([^\\n]+)", output)
    if not m:
        return None
    seg = m.group(1)
    idm = re.search(r"([0-9]+(?:\\.[0-9]+)?)\\s*%?id", seg)
    if idm:
        try:
            idle = float(idm.group(1))
            return max(0.0, min(100.0, 100.0 - idle))
        except Exception:
            return None
    usm = re.search(r"([0-9]+(?:\\.[0-9]+)?)\\s*%?us", seg)
    sym = re.search(r"([0-9]+(?:\\.[0-9]+)?)\\s*%?sy", seg)
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
    entries: List[Dict[str, Any]] = []
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


def _collect_cpu_metric(core_api: client.CoreV1Api, namespace: str, pod_name: str, cname: str, cont_res: Dict[str, Any]) -> None:
    """Populate CPU metric for container."""
    code, out, err = _safe_exec_command(core_api, namespace, pod_name, cname, ["top -b -n1 | head -n 5"])
    if code == 0 and out:
        cont_res["cpu"] = {"util_percent": _parse_cpu_from_top(out), "raw": out.splitlines()[:6]}
    else:
        cont_res["errors"].append({"component": "cpu", "error": err})


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


def _collect_single_container_metrics(core_api: client.CoreV1Api, namespace: str, pod_name: str, cname: str) -> Dict[str, Any]:
    """Collect CPU, memory, disk, and SCTP metrics for a single container."""
    cont_res: Dict[str, Any] = {"cpu": None, "memory": None, "disks": None, "sctp": None, "errors": []}
    _collect_cpu_metric(core_api, namespace, pod_name, cname, cont_res)
    _collect_memory_metric(core_api, namespace, pod_name, cname, cont_res)
    _collect_disk_metric(core_api, namespace, pod_name, cname, cont_res)
    _collect_sctp_metric(core_api, namespace, pod_name, cname, cont_res)
    return cont_res


def _collect_container_system_metrics(core_api: client.CoreV1Api, namespace: str, pod: Dict[str, Any]) -> Dict[str, Any]:
    """Collect per-container system metrics for a pod."""
    pod_name = pod.get("name")
    containers = _container_names_from_pod(pod)
    results: Dict[str, Any] = {}
    for cname in containers:
        results[cname] = _collect_single_container_metrics(core_api, namespace, pod_name, cname)
    return results


# PUBLIC_INTERFACE
def collect_logs_and_metrics(core_api: client.CoreV1Api, namespace: str, pods: List[Dict[str, Any]], max_bytes: int = 50000) -> Dict[str, Any]:
    """Fetch recent logs for DU pods and parse metrics, plus per-container system metrics via exec."""
    from kubernetes.client import ApiException  # lazy import for typing/runtime
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
            container_metrics = _collect_container_system_metrics(core_api, namespace, p)

            aggregated["pod_metrics"][name] = {"radio": radio_metrics, "containers": container_metrics}
            _append_radio_anomalies(name, radio_metrics, aggregated)
            _append_resource_anomalies(name, container_metrics, aggregated)
        except ApiException as exc:
            aggregated["pod_metrics"][name] = {"error": str(exc)}
            aggregated["anomalies"].append({"pod": name, "reason": "log_or_exec_error", "error": str(exc)})
    return aggregated


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
