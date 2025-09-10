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

The app now supports environment-specific YAML config files with environment variable fallback.

- Config directory (default): HealthCheckScriptContainer/configs
- File naming: config_<env>.yaml (e.g., config_dev.yaml, config_stage.yaml, config_prod.yaml)
- Selection priority:
  1) CLI overrides and explicit flags
  2) Environment variables (e.g., VAULT_TOKEN)
  3) YAML file values for the selected environment
  4) Built-in defaults

Selecting environment:
- Use `--env dev|stage|prod` on CLI (or `--environment` for backward-compat)
- Or set ENVIRONMENT=dev|stage|prod
- Optional override config dir via CONFIG_DIR env var

Example:
```
python main.py --site-id site-001 --env stage
```

Sensitive values like tokens can be omitted from YAML and provided via environment variables.

Key environment variables (fallbacks/overrides):
- ENVIRONMENT, LOG_LEVEL, SITE_ID
- VAULT_ADDR, VAULT_TOKEN, VAULT_KUBECONFIG_PATH
- KAFKA_BOOTSTRAP_SERVERS, KAFKA_HEALTH_TOPIC
- LOKI_URL, LOKI_TENANT_ID, LOKI_LABELS
- NODE_LABEL_SELECTOR, POD_LABEL_SELECTOR, K8S_NAMESPACE
- CU_HOST, CU_PORT, RU_HOST, RU_PORT, CONNECTIVITY_TIMEOUT
- CONFIG_DIR (optional, path to configs)

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
There are two supported ways to schedule/trigger the health check via Airflow:

1) PythonOperator (recommended)
- Import the Airflow-compatible callable:
```
from app.airflow_entrypoint import run_healthcheck
result = run_healthcheck(site_id="site-001", environment="stage")
```

2) BashOperator (CLI)
- Execute the CLI directly:
```
python {{ var.value.du_healthcheck_path }}/main.py --site-id site-001 --env stage
# --environment is also accepted (backward-compat)
```

DAG example:
- An example DAG file is provided at:
  HealthCheckScriptContainer/assets/airflow_healthcheck_dag.py
- Copy this file to your Airflow DAGs directory (e.g., $AIRFLOW_HOME/dags/).
- It demonstrates both PythonOperator and BashOperator usage.
- Adjust:
  - schedule_interval (e.g., "*/30 * * * *" for every 30 minutes)
  - site id (DU_HEALTHCHECK_SITE_ID env or inline variables)
  - environment (DU_HEALTHCHECK_ENV env or inline variables)
  - operator selection (PythonOperator, BashOperator, or both)
  - paths (DU_HEALTHCHECK_PATH pointing to a folder containing main.py)

Secrets and configuration:
- Provide secrets (e.g., VAULT_TOKEN) via Airflow Variables/Connections or environment variables in the DAG/task.
- The app can load environment-specific configs from configs/config_<env>.yaml and merge with env vars at runtime.

## Notes
- The previous Flask app and HTTP endpoints have been removed.
- OpenAPI docs and /docs are not applicable anymore.
- External systems (Vault, Kubernetes, Kafka, Loki) must be reachable.