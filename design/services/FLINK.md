# Flink

## 1. Role

Apache Flink runs the **stateful streaming alert job** (`analysis/stream/price_alert_job.py`). It consumes both price topics, partitions by symbol via `key_by`, and evaluates per-tick alert rules with a `KeyedProcessFunction`.

This is the only alerter in the pipeline. An earlier stateless Python version (`consumers/alert_consumer.py`, Phase 5) was removed once Flink covered the same rules — see `design/DESIGN.md` §3.4 for the rationale.

Phase 13 (planned) will add `VolatilityBurstJob` here.

## 2. Containers

| Service | Image | Ports | Role |
|---|---|---|---|
| `flink-jobmanager` | custom (`docker/flink.Dockerfile`) | `8081:8081` (Web UI) | Coordinator, REST API, job submission |
| `flink-taskmanager` | same custom image | — | Executor (`taskmanager.numberOfTaskSlots: 4`) |

Both built from `docker/flink.Dockerfile`:
- Base: `flink:2.0-java17`
- Bakes in PyFlink 2.0.0, JDK headers (PyFlink build script needs them), and the Kafka SQL connector JAR (`flink-sql-connector-kafka-4.0.1-2.0.jar`) into `$FLINK_HOME/lib/`

## 3. Configuration

From `docker/docker-compose.yml`:

- **`FLINK_PROPERTIES: jobmanager.rpc.address: flink-jobmanager`** — TaskManager finds JobManager via Docker DNS.
- **`taskmanager.numberOfTaskSlots: 4`** — one TaskManager × 4 slots. Each parallel subtask of a job takes one slot.
- **`KAFKA_BOOTSTRAP_SERVERS: kafka:29092`** — internal listener (jobs run inside the Docker network, never the host).
- **Volume:** `..:/opt/project` — entire repo mounted so `flink run --python /opt/project/...` can submit jobs without rebuilding the image.

## 4. Submitting the Alert Job

```bash
cd mekong-jobs && make run-flink-alert
# expands to:
docker exec flink-jobmanager flink run --python /opt/project/main.py -- flink-alert
```

The Kafka connector JAR is already in `$FLINK_HOME/lib/`, so the job code doesn't need `env.add_jars()`. The job's `pipeline.jars` config is empty by design.

`PYTHONPATH=/opt/project` is set in the Flink containers via `mekong-infra/docker-compose.yml` so `jobs.*` imports resolve correctly.

## 5. Job Topology

```
stock.price.realtime  ──┐
                          ├──► KafkaSource ──► flat_map ──► key_by(symbol)
crypto.price.realtime ──┘                                       │
                                                                  ▼
                                                       KeyedProcessFunction
                                                       (PriceAlertFn from config/alerts.json)
                                                                  │
                                                                  ▼
                                                              print sink (console)
```

Alert rules come from `config/alerts.json` and are baked into the operator state at submit time. Changing rules requires resubmitting the job.

## 6. Operations

| Task | Command |
|---|---|
| Build image | `cd mekong-infra && make build-flink` |
| Start cluster | `cd mekong-infra && make flink-up` |
| Submit job | `cd mekong-jobs && make run-flink-alert` |
| Web UI | http://localhost:8081 — view running jobs, slot usage, backpressure, checkpoints |
| List jobs | `docker exec flink-jobmanager flink list` |
| Cancel job | `docker exec flink-jobmanager flink cancel <jobid>` |
| Logs | `docker logs flink-jobmanager` / `docker logs flink-taskmanager` |
| Tear down | `cd mekong-infra && docker compose stop flink-jobmanager flink-taskmanager` |

## 7. Failure Modes & Gotchas

- **TaskManager OOM.** With 4 slots, a memory-hungry job (large state) can OOM the JVM. Reduce slots or bump `taskmanager.memory.process.size` via `FLINK_PROPERTIES`.
- **JobManager restart loses jobs.** No HA configured in this dev setup; jobs are not persisted across restarts. Re-submit after restart.
- **Stale connector JAR.** If you bump `apache-flink` in the Dockerfile, also bump the Kafka connector JAR URL — version mismatches surface as cryptic ClassNotFoundException.
- **PyFlink wheel build failure.** The Dockerfile copies JDK headers from `eclipse-temurin:17-jdk` because `flink:2.0-java17` ships a JRE only. Removing that COPY breaks the image build.
- **Wrong Kafka listener.** Jobs running inside the Flink container must use `kafka:29092`, never `localhost:9092`. The compose env sets this correctly; don't override in job code.

## 8. References

- `mekong-jobs`: `jobs/stream/price_alert_job.py` — the only Flink job
- `mekong-jobs`: `jobs/utils.py::evaluate_rules` — shared rule-evaluation function (unit-tested in `tests/unit/`)
- `mekong-infra`: `docker/flink.Dockerfile`
- `mekong-infra`: `config/alerts.json`
- `design/DESIGN.md` §3.4 (alert job rationale)
