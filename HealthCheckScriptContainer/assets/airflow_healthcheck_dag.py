"""
Airflow DAG: DU Health Check (Example)

This DAG demonstrates two ways to schedule/trigger the DU health check script:
1) PythonOperator: imports and calls the Airflow-compatible callable run_healthcheck
   from app.airflow_entrypoint.
2) BashOperator (optional): executes the CLI directly with --site-id and --env.

Adjust schedule, site_id, environment, and operator selection based on your deployment needs.

Placement:
- Copy this file into your Airflow DAGs folder (e.g., $AIRFLOW_HOME/dags/).
- Ensure the Python path and working directory allow importing 'app.airflow_entrypoint'
  or update 'sys.path' below to include the container root if needed.

Environment/Configuration:
- The application relies on environment variables and configs under configs/config_<env>.yaml.
- For production DAGs, supply secrets (e.g., VAULT_TOKEN) via Airflow Connections/Variables
  or environment variables in the Airflow runtime.

Schedule:
- Default schedule is "*/30 * * * *" (every 30 minutes). Adjust 'schedule_interval' as desired.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Optional: If Airflow cannot import your package modules by default,
# uncomment and adjust to add the repo/container root to sys.path
# import sys
# sys.path.append("/usr/local/airflow/dags/HealthCheckScriptContainer")

# -----------------------------------------------------------------------------
# Configuration: adjust to your environment
# -----------------------------------------------------------------------------
DAG_ID = os.getenv("DU_HEALTHCHECK_DAG_ID", "du_healthcheck_example")
# Cron or preset (e.g., '@hourly'). Default: every 30 minutes
SCHEDULE = os.getenv("DU_HEALTHCHECK_SCHEDULE", "*/30 * * * *")
# Start date for Airflow scheduling
START_DATE = datetime(2025, 1, 1)
# Airflow owner and retries
DEFAULT_ARGS = {
    "owner": os.getenv("DU_HEALTHCHECK_OWNER", "ranops"),
    "retries": int(os.getenv("DU_HEALTHCHECK_RETRIES", "0")),
    "retry_delay": timedelta(minutes=int(os.getenv("DU_HEALTHCHECK_RETRY_DELAY_MIN", "5"))),
}

# Health check parameters
SITE_ID = os.getenv("DU_HEALTHCHECK_SITE_ID", "site-001")
# Environment: dev|stage|prod; matches configs/config_<env>.yaml
ENVIRONMENT = os.getenv("DU_HEALTHCHECK_ENV", "stage")

# Optional: where main.py resides if using BashOperator
# You can also set an Airflow Variable and reference with Jinja if preferred
# e.g., "{{ var.value.du_healthcheck_path }}"
HEALTHCHECK_PATH = os.getenv("DU_HEALTHCHECK_PATH", "/usr/local/airflow/dags/HealthCheckScriptContainer")

# -----------------------------------------------------------------------------
# Python callable wrapper.
# This keeps Airflow task code clean and shows a simple example of capturing return data.
# -----------------------------------------------------------------------------
def run_healthcheck_callable(**context):
    """
    PythonOperator task callable.

    Imports and invokes the Airflow-compatible callable from app.airflow_entrypoint:
        from app.airflow_entrypoint import run_healthcheck

    Parameters are received from the DAG-level SITE_ID and ENVIRONMENT.
    The return value can be pushed as XCom (depending on Airflow config).
    """
    from app.airflow_entrypoint import run_healthcheck  # Import inside callable for Airflow scheduler safety

    result = run_healthcheck(site_id=SITE_ID, environment=ENVIRONMENT)
    # Optionally push status to XCom for downstream tasks
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="du_health_status", value=result.get("status"))
    return result

# -----------------------------------------------------------------------------
# DAG definition
# -----------------------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    description="DU Health Check DAG (example) - runs health checks via PythonOperator and/or BashOperator.",
    schedule_interval=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["du", "healthcheck", "k8s", "ran", "example"],
) as dag:

    # Option A: PythonOperator - preferred for native Python integration
    run_healthcheck_python = PythonOperator(
        task_id="run_healthcheck_python",
        python_callable=run_healthcheck_callable,
        provide_context=True,
    )

    # Option B: BashOperator - call the CLI directly (uncomment to enable)
    # Notes:
    # - Adjust HEALTHCHECK_PATH to absolute path containing main.py.
    # - Use --env or --environment (both are supported).
    # - Consider exporting necessary env vars (e.g., VAULT_TOKEN) securely via Airflow.
    run_healthcheck_bash = BashOperator(
        task_id="run_healthcheck_bash",
        bash_command=(
            f"python {HEALTHCHECK_PATH}/main.py "
            f"--site-id {SITE_ID} "
            f"--env {ENVIRONMENT} "
            # Additional options can be appended here:
            # f"--namespace du-namespace --node-label-selector 'ran.site=true' --pod-label-selector 'du=true' "
            # "--no-kafka --no-loki "
        ),
        env={
            # Example: provide secrets/config via environment (ensure these are set securely)
            # "VAULT_ADDR": os.getenv("VAULT_ADDR", ""),
            # "VAULT_TOKEN": os.getenv("VAULT_TOKEN", ""),
            # "VAULT_KUBECONFIG_PATH": os.getenv("VAULT_KUBECONFIG_PATH", ""),
            # "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
            # "LOKI_URL": os.getenv("LOKI_URL", ""),
            # "LOKI_TENANT_ID": os.getenv("LOKI_TENANT_ID", ""),
            # "K8S_NAMESPACE": os.getenv("K8S_NAMESPACE", "default"),
        },
    )

    # Choose your operator:
    # - Use only PythonOperator: run_healthcheck_python
    # - Use only BashOperator: run_healthcheck_bash
    # - Or run both; here we demonstrate Python first then Bash
    run_healthcheck_python >> run_healthcheck_bash
