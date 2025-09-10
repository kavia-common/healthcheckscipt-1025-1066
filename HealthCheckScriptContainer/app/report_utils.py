import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


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
