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

Configuration files:
- The app loads `configs/config_<env>.yaml` automatically based on `--env`/`--environment` or `ENVIRONMENT`.
- Any missing values in the file can be supplied via environment variables (e.g., VAULT_TOKEN).
- Override the config directory by setting `CONFIG_DIR=/path/to/configs`.
