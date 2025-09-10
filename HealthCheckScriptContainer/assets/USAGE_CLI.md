# CLI Usage Examples

Basic:
```
python main.py
```

With site and environment:
```
python main.py --site-id site-001 --environment stage
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
