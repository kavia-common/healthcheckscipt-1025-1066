import json
import logging
import os
import sys
import time
import uuid
from contextvars import ContextVar
from typing import Any, Dict, Optional

# Context variables for correlation and metadata enrichment
trace_id_ctx: ContextVar[str] = ContextVar("trace_id", default="")
site_id_ctx: ContextVar[Optional[str]] = ContextVar("site_id", default=None)
environment_ctx: ContextVar[str] = ContextVar("environment", default="")
namespace_ctx: ContextVar[str] = ContextVar("namespace", default="")
component_ctx: ContextVar[str] = ContextVar("component", default="du-healthcheck")


class JsonFormatter(logging.Formatter):
    """JSON formatter that emits structured log records with contextual metadata."""

    def _base_payload(self, record: logging.LogRecord) -> Dict[str, Any]:
        return {
            "ts": int(time.time() * 1000),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "trace_id": getattr(record, "trace_id", "") or trace_id_ctx.get(),
            "site_id": getattr(record, "site_id", None),
            "environment": getattr(record, "environment", None),
            "namespace": getattr(record, "namespace", None),
            "component": getattr(record, "component", None) or component_ctx.get(),
        }

    def _enrich_context_defaults(self, payload: Dict[str, Any]) -> None:
        if payload.get("site_id") is None:
            payload["site_id"] = site_id_ctx.get()
        if not payload.get("environment"):
            payload["environment"] = environment_ctx.get()
        if not payload.get("namespace"):
            payload["namespace"] = namespace_ctx.get()

    def _attach_process_meta(self, record: logging.LogRecord, payload: Dict[str, Any]) -> None:
        payload["pid"] = os.getpid()
        payload["hostname"] = os.getenv("HOSTNAME", "")
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

    def _attach_extra(self, record: logging.LogRecord, payload: Dict[str, Any]) -> None:
        for k, v in record.__dict__.items():
            if k in payload or k in ("msg", "args", "exc_info", "exc_text", "stack_info", "stacklevel"):
                continue
            try:
                json.dumps({k: v})
                payload[k] = v
            except Exception:
                payload[k] = str(v)

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = self._base_payload(record)
        self._enrich_context_defaults(payload)
        self._attach_process_meta(record, payload)
        self._attach_extra(record, payload)
        return json.dumps(payload, ensure_ascii=False)


def _ensure_stream_handler(logger: logging.Logger, level: int) -> None:
    """Ensure a single stream handler with JSON formatter is attached."""
    # Remove existing handlers to avoid duplicates
    for h in list(logger.handlers):
        logger.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False


# PUBLIC_INTERFACE
def setup_logging(level: str = "INFO") -> logging.Logger:
    """Initialize and return a configured logger that emits JSON structured logs.

    The logger will automatically include contextual metadata such as trace_id,
    site_id, environment, namespace, and component.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger = logging.getLogger("du-healthcheck")
    _ensure_stream_handler(logger, log_level)
    return logger


# PUBLIC_INTERFACE
def new_trace_id() -> str:
    """Generate a new unique trace/session ID."""
    return str(uuid.uuid4())


# PUBLIC_INTERFACE
class LogContext:
    """Context manager to set and propagate logging context (trace_id, site_id, environment, namespace)."""

    def __init__(
        self,
        trace_id: Optional[str] = None,
        site_id: Optional[str] = None,
        environment: Optional[str] = None,
        namespace: Optional[str] = None,
        component: Optional[str] = None,
    ):
        self._tokens: Dict[str, Any] = {}
        self._vals = {
            "trace_id": trace_id or new_trace_id(),
            "site_id": site_id,
            "environment": environment,
            "namespace": namespace,
            "component": component or "du-healthcheck",
        }

    def __enter__(self):
        self._tokens["trace_id"] = trace_id_ctx.set(self._vals["trace_id"])
        if self._vals["site_id"] is not None:
            self._tokens["site_id"] = site_id_ctx.set(self._vals["site_id"])
        if self._vals["environment"] is not None:
            self._tokens["environment"] = environment_ctx.set(self._vals["environment"])  # type: ignore
        if self._vals["namespace"] is not None:
            self._tokens["namespace"] = namespace_ctx.set(self._vals["namespace"])  # type: ignore
        self._tokens["component"] = component_ctx.set(self._vals["component"])  # type: ignore
        return self

    def __exit__(self, exc_type, exc, tb):
        # Reset context vars to previous values
        for key, token in self._tokens.items():
            if key == "trace_id":
                trace_id_ctx.reset(token)  # type: ignore
            elif key == "site_id":
                site_id_ctx.reset(token)  # type: ignore
            elif key == "environment":
                environment_ctx.reset(token)  # type: ignore
            elif key == "namespace":
                namespace_ctx.reset(token)  # type: ignore
            elif key == "component":
                component_ctx.reset(token)  # type: ignore

    # PUBLIC_INTERFACE
    def as_dict(self) -> Dict[str, Any]:
        """Return the context values (useful for passing as extra=...)."""
        return dict(self._vals)


# PUBLIC_INTERFACE
def log_with(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    """Emit a structured log with event name and additional fields using current context."""
    extra = {
        "event": event,
        "trace_id": trace_id_ctx.get(),
        "site_id": site_id_ctx.get(),
        "environment": environment_ctx.get(),
        "namespace": namespace_ctx.get(),
        "component": component_ctx.get(),
    }
    extra.update(fields or {})
    logger.log(level, event, extra=extra)
