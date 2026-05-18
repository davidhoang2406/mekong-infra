# Kafka

## 1. Role

Apache Kafka is the central message bus for live market ticks. Producers push price snapshots; multiple consumers (storage, alert, Flink) read the same stream independently, so adding a new downstream never increases load on the upstream APIs.

Two topics carry all live data:

| Topic | Partitions | Key | Cadence | Producer | Consumers |
|---|---|---|---|---|---|
| `stock.price.realtime` | 6 | symbol (e.g. `VCB`) | ~30 s | `producers/stock_price_producer.py` (vnstock KBS) | `storage_consumer`, `PriceAlertJob` (Flink) |
| `crypto.price.realtime` | 6 | pair with `-` (e.g. `BTC-USDT`) | 5–60 s | `producers/crypto_price_producer.py` (CCXT/Binance) | same |

Both use replication factor 1 (single-broker setup).

## 2. Containers

| Service | Image | Ports | Notes |
|---|---|---|---|
| `kafka` | `apache/kafka:4.0.0` | `9092:9092` (host) | KRaft mode — no ZooKeeper. Single broker, also acts as controller. Internal listener at `kafka:29092`. |
| `kafka-ui` | `ghcr.io/kafbat/kafka-ui:latest` | `8080:8080` | Topic/partition/message browser at http://localhost:8080 |

## 3. Listener Topology

Kafka exposes **two listeners** because Python clients run on the host while Flink/Spark run inside Docker:

| Listener | Bound to | Advertised as | Used by |
|---|---|---|---|
| `PLAINTEXT_HOST` | `0.0.0.0:9092` | `localhost:9092` | Python producers/consumers on the host |
| `PLAINTEXT` | `0.0.0.0:29092` | `kafka:29092` | Containers (Flink, kafka-ui) reaching Kafka over the Docker network |

If you run a consumer from inside another container, point it at `kafka:29092`, not `localhost:9092` — DNS resolution differs.

## 4. Configuration Reference

From `docker/docker-compose.yml`:

- **Process roles:** `broker,controller` (single node)
- **Cluster ID:** hardcoded (`MkU3OEVBNTcwNTJENDM2Qk`) — fine for dev; never reuse in prod
- **Replication factors:** all `1` (offsets topic, transaction state log, ISR) — single broker can't replicate
- **`KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0`** — consumers join immediately, useful for fast dev cycles
- **Volume:** `kafka_data:/var/lib/kafka/data` — persists topics across container restarts. `make uninstall` (selecting Kafka) deletes this volume permanently.

## 5. Healthcheck

```
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Runs every 15 s with a 30 s start period. `kafka-ui` and `flink-jobmanager` use `condition: service_healthy` against this so they don't race against an uninitialised broker.

## 6. Operations

| Task | Command |
|---|---|
| Start | `cd mekong-infra && make install` (interactive) or `docker compose up -d kafka kafka-ui` |
| Create topics | `cd mekong-infra && make topics-create` (idempotent — uses `--if-not-exists`) |
| List topics | `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| Inspect messages | Kafka UI at http://localhost:8080, or `kafka-console-consumer.sh` inside the container |
| Tear down (destroys data) | `docker compose down -v` (removes volumes) |

## 7. Failure Modes & Gotchas

- **Wrong listener from inside Docker.** Symptom: producer/consumer hangs forever connecting. Cause: code using `localhost:9092` from inside a container. Fix: use `kafka:29092`.
- **Topic auto-creation is disabled.** Messages to a non-existent topic fail. Always run `make topics-create` first.
- **Single-broker means no fault tolerance.** Volume corruption = total data loss. Acceptable for dev; do not model production after this.
- **Cluster ID is hardcoded.** If you spin up two Kafka clusters on the same Docker network they will conflict. Change `CLUSTER_ID` if cloning the setup.
- **Stale volume after compose down/up.** If `kafka_data` survives but the broker config changed incompatibly, broker won't start. Tear down the volume (`make uninstall`) and recreate.

## 8. Message Schema

Both topics carry the same JSON envelope (defined in `schemas/message.py`):

```json
{
  "event_type": "price.snapshot",
  "symbol": "BTC/USDT",
  "exchange": "BINANCE",
  "timestamp": "2026-05-17T10:30:00+00:00",
  "source": "ccxt/binance",
  "payload": {
    "price": 62500.0,
    "change": 1200.0,
    "pct_change": 1.96,
    "volume": 1234567890,
    "bid": 62490.0,
    "ask": 62510.0
  }
}
```

`source` is the only field where stock vs crypto differs (`vnstock/KBS` vs `ccxt/binance`); the payload shape is identical so consumers don't special-case.

## 9. References

- `mekong-kafka`: `producers/base_producer.py`, `producers/stock_price_producer.py`, `producers/crypto_price_producer.py`
- `mekong-kafka`: `consumers/base_consumer.py`, `consumers/storage_consumer.py`
- `mekong-jobs`: `jobs/stream/price_alert_job.py` (Flink)
- `design/DESIGN.md` §3–§4 (topic + schema rationale)
