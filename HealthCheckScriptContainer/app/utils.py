"""
Thin compatibility layer exposing selected utilities while delegating to dedicated modules.
This file intentionally remains small and re-exports key PUBLIC_INTERFACE functions.
"""
import logging

from .logging_utils import setup_logging as setup_structured_logging

# PUBLIC_INTERFACE
def setup_logging(level: str = "INFO") -> logging.Logger:
    """Initialize and return a configured logger (structured JSON)."""
    return setup_structured_logging(level)


# Re-export PUBLIC_INTERFACE functions from refactored modules, preserving original import paths.
from .vault_utils import fetch_kubeconfig_from_vault  # noqa: E402
from .kube_utils import build_k8s_client_from_kubeconfig, list_nodes, list_pods, _safe_exec_command  # noqa: E402
from .metrics_utils import collect_logs_and_metrics  # noqa: E402
from .report_utils import build_health_report, HealthResult  # noqa: E402
from .connectivity_utils import tcp_connectivity_check  # noqa: E402
from .publishers import send_to_kafka, push_to_loki  # noqa: E402

__all__ = [
    "setup_logging",
    "fetch_kubeconfig_from_vault",
    "build_k8s_client_from_kubeconfig",
    "list_nodes",
    "list_pods",
    "collect_logs_and_metrics",
    "tcp_connectivity_check",
    "build_health_report",
    "HealthResult",
    "send_to_kafka",
    "push_to_loki",
    "_safe_exec_command",
]
