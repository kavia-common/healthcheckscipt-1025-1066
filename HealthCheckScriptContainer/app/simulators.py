"""
Simulation adapters for external systems (Kubernetes, Vault, Kafka, Loki)
used by the DU Health Check application.

When simulation mode is enabled, these classes replace real integrations with
predictable, deterministic behavior suitable for development and CI.

Design:
- Each simulator exposes a PUBLIC_INTERFACE compatible with the real functions.
- Log messages clearly indicate simulation is active for traceability.

Usage:
- Toggle via SIMULATION_MODE=true env var or --simulate CLI flag.
- The main workflow will select these adapters in simulation mode.
"""
import logging

import time
from typing import Any, Dict, List, Optional, Tuple

from .logging_utils import log_with

# -------------------------- Vault Simulator --------------------------

# PUBLIC_INTERFACE
def simulated_fetch_kubeconfig_from_vault(
    vault_addr: str, token: str, secret_path: str, timeout: float = 8.0
) -> str:
    """Return a fabricated kubeconfig string for simulation.

    Args:
        vault_addr: Ignored; logged only for traceability.
        token: Ignored; simulate auth success.
        secret_path: Ignored; simulate secret path.
        timeout: Ignored; simulate request latency.

    Returns:
        A minimal kubeconfig YAML content that is syntactically correct enough
        for the mocked Kubernetes client usage in simulation.
    """
    logger = logging.getLogger("du-healthcheck")
    log_with(
        logger, logging.INFO, event="sim_vault_request",
        note="simulation_mode_active", addr=vault_addr, secret_path=secret_path
    )
    # Minimal dummy kubeconfig (never used to contact a real cluster in sim mode)
    kubeconfig = """apiVersion: v1
clusters:
- cluster:
    server: https://127.0.0.1:6443
  name: sim-cluster
contexts:
- context:
    cluster: sim-cluster
    user: sim-user
  name: sim-context
current-context: sim-context
kind: Config
preferences: {}
users:
- name: sim-user
  user:
    token: FAKE
"""
    return kubeconfig

# -------------------------- Kubernetes Simulator --------------------------

class _SimPod:
    def __init__(self, name: str, namespace: str, labels: Dict[str, str], containers: List[Dict[str, Any]]):
        self.name = name
        self.namespace = namespace
        self.labels = labels
        self.phase = "Running"
        self.hostIP = "10.0.0.10"
        self.podIP = "10.244.0.12"
        self.containers = containers

class _SimCoreApi:
    """Mock of kubernetes.client.CoreV1Api surface we use."""

    def __init__(self, logger: logging.Logger, namespace: str, pod_selector: str):
        self.logger = logger
        self.namespace = namespace
        self.selector = pod_selector
        # Build a small simulated world
        self._pods: List[_SimPod] = [
            _SimPod(
                name="du-processing-0",
                namespace=namespace,
                labels={"app": "du", "du": "true"},
                containers=[
                    {"name": "du-app", "image": "sim/du:latest"},
                    {"name": "sctp-proxy", "image": "sim/sctp:latest"},
                ],
            ),
            _SimPod(
                name="du-radio-0",
                namespace=namespace,
                labels={"app": "du", "du": "true"},
                containers=[{"name": "du-radio", "image": "sim/radio:latest"}],
            ),
        ]

    # Methods mirrored by our code paths
    def list_node(self, label_selector: Optional[str] = None):
        class _Obj:
            def __init__(self, items):
                self.items = items
        # One healthy node, one not-ready node (to see behavior)
        Node = type("Node", (), {})
        Cond = type("Cond", (), {})
        Meta = type("Meta", (), {})
        Status = type("Status", (), {})
        Alloc = type("Alloc", (), {})
        n1 = Node()
        n1.metadata = Meta()
        n1.metadata.name = "node-a"
        n1.metadata.labels = {"ran.site": "true", "site_id": "site-001"}
        n1.status = Status()
        ready = Cond(); ready.type = "Ready"; ready.status = "True"
        n1.status.conditions = [ready]
        a1 = Alloc(); a1.cpu = "8"; a1.memory = "16Gi"
        n1.status.allocatable = {"cpu": "8", "memory": "16Gi"}

        n2 = Node()
        n2.metadata = Meta()
        n2.metadata.name = "node-b"
        n2.metadata.labels = {"ran.site": "true", "site_id": "site-001"}
        n2.status = Status()
        notready = Cond(); notready.type = "Ready"; notready.status = "False"
        n2.status.conditions = [notready]
        n2.status.allocatable = {"cpu": "8", "memory": "16Gi"}

        log_with(self.logger, logging.INFO, event="sim_k8s_list_nodes", selector=label_selector)
        return _Obj([n1, n2])

    def list_namespaced_pod(self, namespace: str, label_selector: Optional[str] = None):
        class _Obj:
            def __init__(self, items):
                self.items = items
        # Convert _SimPod to API-ish objects the downstream expects
        Pod = type("Pod", (), {})
        Meta = type("Meta", (), {})
        Status = type("Status", (), {})
        ContStat = type("ContStat", (), {})
        State = type("State", (), {})
        items = []
        for sp in self._pods:
            p = Pod()
            p.metadata = Meta()
            p.metadata.name = sp.name
            p.metadata.namespace = sp.namespace
            p.metadata.labels = sp.labels
            p.status = Status()
            p.status.phase = sp.phase
            p.status.host_ip = sp.hostIP
            p.status.pod_ip = sp.podIP
            # Build container_statuses list
            p.status.container_statuses = []
            for c in sp.containers:
                cs = ContStat()
                cs.name = c["name"]
                cs.image = c["image"]
                cs.ready = True
                cs.restart_count = 0
                st = State()
                st.waiting = None; st.terminated = None
                st.running = object()
                cs.state = st
                p.status.container_statuses.append(cs)
            items.append(p)
        log_with(self.logger, logging.INFO, event="sim_k8s_list_pods", selector=label_selector, namespace=namespace)
        return _Obj(items)

    def read_namespaced_pod_log(self, name: str, namespace: str, tail_lines: int = 500, timestamps: bool = True, _preload_content: bool = True):
        # Provide fake radio log lines with some metrics
        now = int(time.time())
        lines = [
            f"{now} INFO RRC connection established for UE 123",
            f"{now} WARN UL CRC fail on slot 101",
            f"{now} INFO DL MCS avg: 17.5",
            f"{now} INFO srs error detected on port 0",
            f"{now} INFO Normal operation",
        ]
        return "\n".join(lines)

# PUBLIC_INTERFACE
def simulated_build_k8s_client_from_kubeconfig(kubeconfig_content: str) -> Tuple[_SimCoreApi, object]:
    """Return simulated CoreV1Api and AppsV1Api clients."""
    logger = logging.getLogger("du-healthcheck")
    log_with(logger, logging.INFO, event="sim_k8s_client_init", note="simulation_mode_active")
    core = _SimCoreApi(logger, namespace="default", pod_selector="du=true")
    apps = object()
    return core, apps

# PUBLIC_INTERFACE
def simulated_list_nodes(core_api: _SimCoreApi, label_selector: str) -> List[Dict[str, Any]]:
    """Return a fabricated set of nodes with one ready and one not-ready."""
    # Reuse the simulated client's list_node and transform to dicts similar to kube_utils
    items = core_api.list_node(label_selector=label_selector).items
    res: List[Dict[str, Any]] = []
    for n in items:
        conditions = {c.type: c.status for c in (n.status.conditions or [])}
        entry = {
            "name": n.metadata.name,
            "labels": n.metadata.labels or {},
            "allocatable": {k: str(v) for k, v in (getattr(n.status, "allocatable", {}) or {}).items()},
            "conditions": conditions,
            "ready": conditions.get("Ready") == "True",
        }
        res.append(entry)
    return res

# PUBLIC_INTERFACE
def simulated_list_pods(core_api: _SimCoreApi, namespace: str, label_selector: str) -> List[Dict[str, Any]]:
    """Return fabricated pods with container statuses."""
    items = core_api.list_namespaced_pod(namespace=namespace, label_selector=label_selector).items
    pods: List[Dict[str, Any]] = []
    for p in items:
        cs_list = []
        for cs in getattr(p.status, "container_statuses", []) or []:
            cs_list.append({
                "name": cs.name,
                "ready": cs.ready,
                "restarts": cs.restart_count,
                "state": "running",
                "image": cs.image,
            })
        pods.append({
            "name": p.metadata.name,
            "namespace": p.metadata.namespace,
            "phase": p.status.phase,
            "hostIP": p.status.host_ip,
            "podIP": p.status.pod_ip,
            "labels": p.metadata.labels or {},
            "containers": cs_list,
        })
    return pods

# PUBLIC_INTERFACE
def simulated_safe_exec_command(core_api: _SimCoreApi, namespace: str, pod: str, container: str, command: List[str], timeout_seconds: int = 8) -> Tuple[int, str, str]:
    """Simulate exec command responses for CPU, memory, disk, and SCTP."""
    cmd = " ".join(command)
    # Simulate deterministic outputs that parsers can handle
    if "top -b -n1" in cmd:
        # Include Cpu(s) idle so parser can compute util
        output = "top - 10:00:00 up 1 day,  1 user,  load average: 0.10, 0.15, 0.20\n%Cpu(s): 12.3 us, 5.7 sy, 0.0 ni, 80.0 id, 1.5 wa, 0.3 hi, 0.2 si, 0.0 st\n"
        return 0, output, ""
    if "free -m" in cmd:
        output = "              total        used        free      shared  buff/cache   available\nMem:           15936        4096        9216         100         2624        12000\nSwap:           2048           0        2048\n"
        return 0, output, ""
    if "df -h" in cmd:
        output = "Filesystem      Size  Used Avail Use% Mounted on\n/dev/root        50G   20G   28G  42% /\n/dev/sda1       200G  150G   50G  75% /data\n"
        return 0, output, ""
    if "ss -H -t -a -p" in cmd:
        output = "ESTAB  0  0  10.0.0.2:38472   10.0.0.3:38472  users:(\"sctp-proxy\",pid=123,fd=9) sctp\n"
        return 0, output, ""
    if "grep -i sctp /proc/net/protocols" in cmd:
        return 0, "SCTP\t\t143\t\tSCTP\n", ""
    # Unknown commands: simulate slight error
    return 1, "", f"simulated_command_not_found: {cmd}"

# -------------------------- Kafka Simulator --------------------------

# PUBLIC_INTERFACE
def simulated_send_to_kafka(bootstrap_servers: str, topic: str, payload: Dict[str, Any], logger: logging.Logger) -> None:
    """Pretend to send to Kafka; log the action and do nothing."""
    size_bytes = len(str(payload).encode("utf-8"))
    log_with(
        logger, logging.INFO, event="sim_kafka_produce",
        note="simulation_mode_active", bootstrap_servers=bootstrap_servers, topic=topic, size_bytes=size_bytes
    )
    # Simulate a small delay
    time.sleep(0.05)
    log_with(logger, logging.INFO, event="sim_kafka_flush_done")

# -------------------------- Loki Simulator --------------------------

# PUBLIC_INTERFACE
def simulated_push_to_loki(loki_url: str, tenant_id: Optional[str], labels: str, entries: List[Dict[str, Any]], logger: logging.Logger) -> None:
    """Pretend to push to Loki via HTTP; log the intended payload size."""
    log_with(
        logger, logging.INFO, event="sim_loki_push",
        note="simulation_mode_active", url=loki_url, tenant_id=tenant_id, entries=len(entries), labels=labels
    )
    # No-op; emulate quick success
    time.sleep(0.02)
