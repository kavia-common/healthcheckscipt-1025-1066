# Migration to Standalone CLI

This container has been refactored from a Flask-based service to a standalone CLI Python application.

What changed:
- Removed Flask app, routes, and HTTP endpoints.
- Added `main.py` entry point for direct execution or Airflow triggering.
- Retained Airflow-compatible callable: `from app.airflow_entrypoint import run_healthcheck`.
- Requirements slimmed to remove Flask dependencies.

What to update:
- If you previously called HTTP endpoints, replace with CLI invocation:
  - Before: POST http://<host>:8080/system/healthcheck
  - After:  `python main.py --site-id site-001 --environment dev`
- If using Airflow, either:
  - Import callable: `from app.airflow_entrypoint import run_healthcheck`
  - Or run as a shell/py operator using `python main.py ...`

Environment:
- See `.env.example` for required variables (Vault, Kafka, Loki, selectors).
