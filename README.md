# DU Health Check Service (HealthCheckScriptContainer)

This application is a standalone Python script for executing health checks on DU sites in a 5G Kubernetes environment (ORAN 7.2). It:
- Accepts site ID and environment via CLI arguments or environment variables.
- Retrieves kubeconfig securely from Vault.
- Filters K8s nodes/pods by labels.
- Performs node, pod, and container health checks.
- Parses DU logs for radio access metrics.
- Execs into each DU pod container to collect system metrics:
  - CPU utilization via 'top -b -n1' (parsed as 100 - idle)
  - RAM usage via 'free -m' (fallback to /proc/meminfo)
  - Disk utilization via 'df -h'
  - SCTP status via 'ss -H -t -a -p | grep -i sctp' (fallback to /proc)
- Checks TCP connectivity to CU and RU.
- Generates detailed health reports.
- Publishes reports to Kafka and pushes failures/anomalies/logs to Loki.
- Supports configurable log levels and multi-env (dev|stage|prod).
- Structured JSON logging with per-run trace_id for correlation across Vault/K8s/Kafka/Loki calls.
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

## Structured Logging and Traceability

The application uses structured JSON logging with a per-run trace_id for correlation:
- A unique trace_id is generated at the start of each run and is propagated to all logs.
- All major steps emit start/end logs with contextual metadata: site_id, environment, namespace.
- External calls (Vault, Kubernetes, Kafka, Loki) include event names, URLs/topics, status codes, and errors.
- Per-pod and per-container actions (log reads, exec commands for metrics) log detailed context including pod/container names, commands, and outcomes.
- Exceptions are logged with error details and context for easier troubleshooting.

Public helper interfaces:
- setup_logging(level): sets up JSON logger
- LogContext(trace_id, site_id, environment, namespace): context manager to bind correlation fields
- log_with(logger, level, event, **fields): produce structured logs with custom fields

Example log entry:
```
{"ts": 1736530000000, "level": "INFO", "logger": "du-healthcheck", "msg": "run_start", "event": "run_start", "trace_id": "e7f0...", "site_id": "site-001", "environment": "stage", "namespace": "du-stage", "component": "du-healthcheck", "pid": 123, "hostname": "runner-1"}
```

## Health Report Structure (excerpt)

The metrics section now includes per-pod radio metrics and per-container system metrics collected via Kubernetes exec:

```
"metrics": {
  "pod_metrics": {
    "<pod-name>": {
      "radio": {
        "srs_errors": <int>,
        "phy_ul_crc_fail": <int>,
        "phy_dl_mcs_avg": <float|null>,
        "rrc_conn_established": <int>
      },
      "containers": {
        "<container-name>": {
          "cpu": { "util_percent": <float|null>, "raw": ["Cpu(s): ...", "..."] },
          "memory": { "total_mb": <float>, "used_mb": <float>, "free_mb": <float>, "used_percent": <float|null> },
          "disks": [ { "filesystem": "...", "size": "...", "used": "...", "available": "...", "use_percent": "45%", "mountpoint": "/" }, ... ],
          "sctp": { "has_sctp": <bool>, "lines": ["... matching ss output ..."] },
          "errors": [ { "component": "cpu|memory|disk|sctp", "error": "..." }, ... ]
        }
      }
    }
  },
  "anomalies": [
    { "pod": "<pod>", "reason": "radio_metric_anomaly", "metrics": {...} },
    { "pod": "<pod>", "container": "<container>", "reason": "resource_pressure", "cpu_util_percent": 95.0, "mem_used_percent": 92.3 }
  ]
}
```

Notes:
- Commands executed inside containers:
  - CPU: top -b -n1 | head -n 5
  - RAM: free -m (fallback to parsing /proc/meminfo)
  - Disk: df -h
  - SCTP: ss -H -t -a -p | grep -i sctp (fallback grep /proc/net/protocols)
- Errors or absence of tools are captured under the "errors" list per container.
- Anomalies include radio metric anomalies and resource pressure (>90% CPU or memory).

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

## Code-size diagnostics (developer aid)
To measure function sizes and file lengths for refactoring, run:
```
python HealthCheckScriptContainer/tools/line_diagnostics.py
```
This prints:
- Functions exceeding 15 physical code lines (excluding comments/blank lines)
- Percentage of functions with length ≤ 15
- Files exceeding 400 total lines and their lengths
