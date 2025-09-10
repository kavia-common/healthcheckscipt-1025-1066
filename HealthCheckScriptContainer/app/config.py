import os
import json
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # We will handle absence gracefully in loader


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge two dictionaries (override has precedence)."""
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _load_yaml_file(path: str) -> Dict[str, Any]:
    """Load YAML file if exists and YAML lib available, else return {}."""
    if not path or not os.path.exists(path):
        return {}
    if yaml is None:
        # Fallback: attempt very limited JSON parse if file is actually JSON; otherwise ignore
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return {}
    # normalize keys to snake_case style used by dataclass
    return data


def _coalesce_env(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Apply environment variable overrides for known fields if env var present."""
    env_map = {
        "environment": "ENVIRONMENT",
        "site_id": "SITE_ID",
        "log_level": "LOG_LEVEL",
        "vault_addr": "VAULT_ADDR",
        "vault_token": "VAULT_TOKEN",
        "vault_kubeconfig_path": "VAULT_KUBECONFIG_PATH",
        "kafka_bootstrap_servers": "KAFKA_BOOTSTRAP_SERVERS",
        "kafka_health_topic": "KAFKA_HEALTH_TOPIC",
        "loki_url": "LOKI_URL",
        "loki_tenant_id": "LOKI_TENANT_ID",
        "loki_labels": "LOKI_LABELS",
        "node_label_selector": "NODE_LABEL_SELECTOR",
        "pod_label_selector": "POD_LABEL_SELECTOR",
        "namespace": "K8S_NAMESPACE",
        "cu_host": "CU_HOST",
        "cu_port": "CU_PORT",
        "ru_host": "RU_HOST",
        "ru_port": "RU_PORT",
        "connectivity_timeout": "CONNECTIVITY_TIMEOUT",
        "airflow_enabled": "AIRFLOW_ENABLED",
        "request_timeout": "REQUEST_TIMEOUT",
        "config_dir": "CONFIG_DIR",
    }
    for k, env_k in env_map.items():
        if env_k in os.environ and os.environ[env_k] != "":
            val = os.environ[env_k]
            # cast numerics/bools where appropriate
            if k in ("cu_port", "ru_port"):
                try:
                    cfg[k] = int(val)
                except Exception:
                    cfg[k] = cfg.get(k)
            elif k in ("connectivity_timeout", "request_timeout"):
                try:
                    cfg[k] = float(val)
                except Exception:
                    cfg[k] = cfg.get(k)
            elif k in ("airflow_enabled",):
                cfg[k] = str(val).lower() == "true"
            else:
                cfg[k] = val
    return cfg


# PUBLIC_INTERFACE
@dataclass
class AppConfig:
    """Configuration object for HealthCheckScriptContainer supporting multi-env and externalized config via YAML.

    Precedence:
    1) CLI overrides (handled by caller by mutating instance)
    2) Environment variables
    3) YAML file at configs/config_<env>.yaml (env from CLI/ENVIRONMENT)
    4) Built-in defaults
    """

    # selection and general
    environment: str = field(default_factory=lambda: os.getenv("ENVIRONMENT", "dev"))
    site_id: Optional[str] = field(default_factory=lambda: os.getenv("SITE_ID"))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    # Vault
    vault_addr: Optional[str] = field(default_factory=lambda: os.getenv("VAULT_ADDR"))
    vault_token: Optional[str] = field(default_factory=lambda: os.getenv("VAULT_TOKEN"))
    vault_kubeconfig_path: Optional[str] = field(default_factory=lambda: os.getenv("VAULT_KUBECONFIG_PATH"))

    # Kafka
    kafka_bootstrap_servers: Optional[str] = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    kafka_health_topic: str = field(default_factory=lambda: os.getenv("KAFKA_HEALTH_TOPIC", "du.health.reports"))

    # Loki
    loki_url: Optional[str] = field(default_factory=lambda: os.getenv("LOKI_URL"))
    loki_tenant_id: Optional[str] = field(default_factory=lambda: os.getenv("LOKI_TENANT_ID"))
    loki_labels: str = field(default_factory=lambda: os.getenv("LOKI_LABELS", '{app="du-healthcheck"}'))

    # Kubernetes label filters
    node_label_selector: str = field(default_factory=lambda: os.getenv("NODE_LABEL_SELECTOR", "ran.site=true"))
    pod_label_selector: str = field(default_factory=lambda: os.getenv("POD_LABEL_SELECTOR", "du=true"))
    namespace: str = field(default_factory=lambda: os.getenv("K8S_NAMESPACE", "default"))

    # Connectivity targets
    cu_host: Optional[str] = field(default_factory=lambda: os.getenv("CU_HOST"))
    cu_port: int = field(default_factory=lambda: int(os.getenv("CU_PORT", "38472")))
    ru_host: Optional[str] = field(default_factory=lambda: os.getenv("RU_HOST"))
    ru_port: int = field(default_factory=lambda: int(os.getenv("RU_PORT", "22102")))
    connectivity_timeout: float = field(default_factory=lambda: float(os.getenv("CONNECTIVITY_TIMEOUT", "3.0")))

    # Airflow compatibility
    airflow_enabled: bool = field(default_factory=lambda: os.getenv("AIRFLOW_ENABLED", "false").lower() == "true")

    # Misc
    request_timeout: float = field(default_factory=lambda: float(os.getenv("REQUEST_TIMEOUT", "8.0")))

    # Internal: configuration directory
    config_dir: str = field(default_factory=lambda: os.getenv("CONFIG_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "configs")))

    # PUBLIC_INTERFACE
    def load_from_files_and_env(self, env_override: Optional[str] = None) -> None:
        """Load configuration from YAML file for the selected environment, then apply env var overrides.

        Args:
            env_override: Optional environment name to force selection (dev|stage|prod).
        """
        # Determine environment
        if env_override:
            self.environment = env_override
        env = (self.environment or "dev").lower()

        # Build path to YAML file
        cfg_dir = self.config_dir
        filename = f"config_{env}.yaml"
        path = os.path.join(cfg_dir, filename)

        # Start with dataclass defaults
        defaults = asdict(self)

        # Load from YAML if present
        file_cfg = _load_yaml_file(path)

        # Merge in order: file -> defaults to ensure file overrides defaults
        merged = _deep_merge(defaults, file_cfg)

        # Apply env var overrides (highest precedence for this method)
        merged = _coalesce_env(merged)

        # Set attributes
        for k, v in merged.items():
            if hasattr(self, k):
                setattr(self, k, v)

    def _collect_missing(self) -> Dict[str, Any]:
        missing: Dict[str, Any] = {}
        if not self.vault_addr:
            missing["VAULT_ADDR"] = "Vault address required to retrieve kubeconfig"
        if not self.vault_token:
            missing["VAULT_TOKEN"] = "Vault token required to retrieve kubeconfig"
        if not self.vault_kubeconfig_path:
            missing["VAULT_KUBECONFIG_PATH"] = "Vault secret path for kubeconfig is required"
        if not self.kafka_bootstrap_servers:
            missing["KAFKA_BOOTSTRAP_SERVERS"] = "Kafka bootstrap servers required"
        if not self.loki_url:
            missing["LOKI_URL"] = "Loki HTTP push URL required"
        return missing

    def validate(self) -> None:
        """Validate required configuration for runtime (after loading)."""
        missing = self._collect_missing()
        if missing:
            details = ", ".join([f"{k}: {v}" for k, v in missing.items()])
            raise ValueError(f"Invalid configuration. Missing variables: {details}")
