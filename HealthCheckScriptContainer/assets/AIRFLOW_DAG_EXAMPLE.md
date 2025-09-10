# Airflow DAG Example (for reference)

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def run():
    from app.airflow_entrypoint import run_healthcheck
    res = run_healthcheck(site_id="site-001", environment="stage")
    # Optionally push XCom or additional handling
    return res.get("status")

with DAG(
    dag_id="du_healthcheck",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 0, "owner": "ranops"},
) as dag:
    t1 = PythonOperator(task_id="run_healthcheck", python_callable=run)
```
