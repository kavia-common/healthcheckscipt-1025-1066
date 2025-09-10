# DU Health Check Service (HealthCheckScriptContainer)

This application is a standalone Python script for executing health checks on DU sites in a 5G Kubernetes environment (ORAN 7.2). It:
- Accepts site ID and environment via CLI arguments or environment variables.
- Retrieves kubeconfig securely from Vault.
- Filters K8s nodes/pods by labels.
- Performs node, pod, and container health checks.
- Parses DU logs for radio access metrics.
- Checks TCP connectivity to CU and RU.
- Generates detailed health reports.
- Publishes reports to Kafka and pushes failures/anomalies/logs to Loki.
- Supports configurable log levels and multi-env (dev|stage|prod).
- Is schedulable (Airflow-compatible callable). No Flask / web API.

## Configuration
Copy `.env.example` to your environment and set values via your deployment mechanism.

Key variables:
- ENVIRONMENT, LOG_LEVEL, SITE_ID
- VAULT_ADDR, VAULT_TOKEN, VAULT_KUBECONFIG_PATH
- KAFKA_BOOTSTRAP_SERVERS, KAFKA_HEALTH_TOPIC
- LOKI_URL, LOKI_TENANT_ID, LOKI_LABELS
- NODE_LABEL_SELECTOR, POD_LABEL_SELECTOR, K8S_NAMESPACE
- CU_HOST, CU_PORT, RU_HOST, RU_PORT, CONNECTIVITY_TIMEOUT

## Usage - Standalone CLI
Run the health check directly:
```
python main.py --site-id site-001 --environment dev \
  --namespace du-namespace \
  --node-label-selector "ran.site=true,site_id=site-001" \
  --pod-label-selector "du=true" \
  --log-level INFO
```
Flags:
- --site-id: Optional site identifier
- --environment: dev|stage|prod
- --namespace: Kubernetes namespace
- --node-label-selector: Node label selector
- --pod-label-selector: Pod label selector
- --log-level: DEBUG|INFO|WARN|ERROR
- --no-kafka: Disable Kafka publish
- --no-loki: Disable Loki push

Exit codes:
- 0: healthy
- 1: degraded
- 2: fatal error during execution

The script prints a full JSON report to stdout for easy parsing and Airflow log visibility.

## Airflow
You can import and call:
```
from app.airflow_entrypoint import run_healthcheck
result = run_healthcheck(site_id="site-001", environment="stage")
```
Or execute via BashOperator:
```
python {{ var.value.du_healthcheck_path }}/main.py --site-id site-001 --environment stage
```

## Notes
- The previous Flask app and HTTP endpoints have been removed.
- OpenAPI docs and /docs are not applicable anymore.
- External systems (Vault, Kubernetes, Kafka, Loki) must be reachable.