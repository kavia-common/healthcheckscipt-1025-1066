import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests
from confluent_kafka import Producer

from .logging_utils import log_with


# PUBLIC_INTERFACE
def send_to_kafka(bootstrap_servers: str, topic: str, payload: Dict[str, Any], logger: logging.Logger) -> None:
    """Send health report to Kafka topic."""
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": f"du-healthcheck-{os.getenv('HOSTNAME', 'local')}",
        "enable.idempotence": True,
        "compression.type": "zstd",
        "linger.ms": 50,
    }
    log_with(logger, logging.INFO, event="kafka_producer_init", bootstrap_servers=bootstrap_servers, topic=topic)
    producer = Producer(conf)

    def delivery(err, msg):
        if err is not None:
            log_with(logger, logging.ERROR, event="kafka_delivery_failed", error=str(err))
        else:
            log_with(
                logger,
                logging.DEBUG,
                event="kafka_delivery_ok",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    data = json.dumps(payload, default=lambda o: o.__dict__)
    log_with(logger, logging.DEBUG, event="kafka_produce", size_bytes=len(data))
    producer.produce(topic, value=data.encode("utf-8"), callback=delivery)
    producer.flush(5)
    log_with(logger, logging.INFO, event="kafka_flush_done")


# PUBLIC_INTERFACE
def push_to_loki(loki_url: str, tenant_id: Optional[str], labels: str, entries: List[Dict[str, Any]], logger: logging.Logger) -> None:
    """Push logs/entries to Loki via HTTP API."""
    def parse_labels(labels_str: str) -> Dict[str, str]:
        res: Dict[str, str] = {}
        s = labels_str.strip()
        if not (s.startswith("{") and s.endswith("}")):
            return res
        inner = s[1:-1].strip()
        if not inner:
            return res
        parts = [p.strip() for p in inner.split(",")]
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                res[k.strip()] = v.strip().strip('"')
        return res

    stream_labels = parse_labels(labels)
    ns_entries = []
    for e in entries:
        ts_ns = str(int(time.time() * 1e9))
        ns_entries.append([ts_ns, json.dumps(e)])

    payload = {"streams": [{"stream": stream_labels, "values": ns_entries}]}
    headers = {"Content-Type": "application/json"}
    if tenant_id:
        headers["X-Scope-OrgID"] = tenant_id
    url = loki_url.rstrip("/") + "/loki/api/v1/push"
    log_with(logger, logging.INFO, event="loki_push_start", url=url, entries=len(entries))
    try:
        resp = requests.post(url, data=json.dumps(payload), headers=headers, timeout=5)
        if resp.status_code >= 300:
            log_with(logger, logging.ERROR, event="loki_push_failed", status_code=resp.status_code, response=resp.text[:500])
        else:
            log_with(logger, logging.INFO, event="loki_push_ok", status_code=resp.status_code)
    except Exception as exc:
        log_with(logger, logging.ERROR, event="loki_push_error", error=str(exc))
