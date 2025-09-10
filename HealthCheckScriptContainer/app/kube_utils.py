import logging
from typing import Any, Dict, List, Tuple

from kubernetes import client, config as k8s_config
from kubernetes.client import ApiException
from kubernetes.stream import stream

from .logging_utils import log_with


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


def _container_statuses_for_pod(p) -> List[Dict[str, Any]]:
    statuses: List[Dict[str, Any]] = []
    if not getattr(p.status, "container_statuses", None):
        return statuses
    for cs in p.status.container_statuses:
        statuses.append(_single_container_status_dict(cs))
    return statuses


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


# PUBLIC_INTERFACE
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


# PUBLIC_INTERFACE
def _safe_exec_command(
    core_api: client.CoreV1Api,
    namespace: str,
    pod: str,
    container: str,
    command: List[str],
    timeout_seconds: int = 8,
) -> Tuple[int, str, str]:
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
