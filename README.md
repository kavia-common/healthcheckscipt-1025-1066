# DU Health Check Service (HealthCheckScriptContainer)

This service is a Flask-based backend for executing health checks on DU sites in a 5G Kubernetes environment (ORAN 7.2). It:
- Accepts site ID and environment via externalized config (env vars).
- Retrieves kubeconfig securely from Vault.
- Filters K8s nodes/pods by labels.
- Performs node, pod, and container health checks.
- Parses DU logs for radio access metrics.
- Checks TCP connectivity to CU and RU.
- Generates detailed health reports.
- Publishes reports to Kafka and pushes failures/anomalies/logs to Loki.
- Supports configurable log levels and multi-env (dev|stage|prod).
- Is schedulable (Airflow-compatible callable) and exposes system integration endpoints (no UI).

## Configuration
Copy `.env.example` to your environment and set values via deployment mechanism.

Key variables:
- ENVIRONMENT, LOG_LEVEL, SITE_ID
- VAULT_ADDR, VAULT_TOKEN, VAULT_KUBECONFIG_PATH
- KAFKA_BOOTSTRAP_SERVERS, KAFKA_HEALTH_TOPIC
- LOKI_URL, LOKI_TENANT_ID, LOKI_LABELS
- NODE_LABEL_SELECTOR, POD_LABEL_SELECTOR, K8S_NAMESPACE
- CU_HOST, CU_PORT, RU_HOST, RU_PORT, CONNECTIVITY_TIMEOUT

## API (System Integration)
- GET `/` -> Liveness
- POST `/system/healthcheck`
  Body (JSON):
  {
    "site_id": "site-001",
    "environment": "dev",
    "node_label_selector": "ran.site=true,site_id=site-001",
    "pod_label_selector": "du=true",
    "namespace": "du-namespace",
    "airflow_run_id": "manual__2025-01-01T00:00:00"
  }

Response: Full health report JSON. Also publishes to Kafka and logs to Loki.

Docs: OpenAPI at `/docs`

## Airflow
You can import and call:
```
from app.airflow_entrypoint import run_healthcheck
result = run_healthcheck(site_id="site-001", environment="stage")
```

## Local run
```
python -m app.airflow_entrypoint
```

Or start Flask for system integration endpoints:
```
python run.py
```

Note: External systems (Vault, Kubernetes, Kafka, Loki) must be reachable.