# CLI Usage Examples

Basic:
```
python main.py
```

Select environment (new):
```
python main.py --env dev
# or (backward-compat)
python main.py --environment stage
```

With site and environment:
```
python main.py --site-id site-001 --env stage
```

With selectors and namespace:
```
python main.py --namespace du-namespace \
  --node-label-selector "ran.site=true,site_id=site-001" \
  --pod-label-selector "du=true" --log-level DEBUG
```

Disable Kafka/Loki (dry-run for network publishing):
```
python main.py --no-kafka --no-loki
```

Simulation mode (mock all external systems):
```
# Via CLI flag
python main.py --env dev --simulate

# Or via environment variable
SIMULATION_MODE=true python main.py --env dev
```
In simulation mode, Kubernetes/Vault/Kafka/Loki are replaced with simulators; logs will indicate simulation is active.

Configuration files:
- The app loads `configs/config_<env>.yaml` automatically based on `--env`/`--environment` or `ENVIRONMENT`.
- Any missing values in the file can be supplied via environment variables (e.g., VAULT_TOKEN).
- Override the config directory by setting `CONFIG_DIR=/path/to/configs`.

Container-level metrics:
- For each DU pod container, the app runs the following commands via Kubernetes exec:
  - CPU: `top -b -n1 | head -n 5` parsed as 100 - idle
  - RAM: `free -m` (fallback to `/proc/meminfo`)
  - Disk: `df -h`
  - SCTP: `ss -H -t -a -p | grep -i sctp` (fallback to checking `/proc/net/protocols`)
- If a command is not available in the image, the error is captured in the report under `metrics.pod_metrics[<pod>].containers[<container>].errors`.
