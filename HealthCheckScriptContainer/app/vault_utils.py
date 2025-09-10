import logging
from typing import Any, Dict, List, Optional

import requests

from .logging_utils import log_with, trace_id_ctx


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
