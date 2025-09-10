import os
from dataclasses import dataclass, field
from typing import Optional, Dict, Any


# PUBLIC_INTERFACE
@dataclass
class AppConfig:
    """Configuration object for HealthCheckScriptContainer supporting multi-env and externalized config."""
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

    def validate(self) -> None:
        """Validate required configuration for runtime."""
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

        if missing:
            details = ", ".join([f"{k}: {v}" for k, v in missing.items()])
            raise ValueError(f"Invalid configuration. Missing variables: {details}")
