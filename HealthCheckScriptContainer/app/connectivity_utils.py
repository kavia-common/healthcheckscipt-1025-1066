import logging
import socket
import time
from typing import Any, Dict, Optional

from .logging_utils import log_with


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
