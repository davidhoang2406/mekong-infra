# Improvement Suggestions

> Status: planning only. Nothing below should be coded until a phase is selected and scoped.
> Last updated: 2026-05-18 — reflects Phase 10 (Dagster) complete, Phases 11–13 planned.

---

## 1. Operational Improvements

### 1.1 No CI/CD across any repo

None of the six repos have GitHub Actions. Broken imports and failed tests are only caught locally. A minimal workflow per repo:

```yaml
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-python@v5
      - run: pip install -r requirements.txt
      - run: pytest -m unit
```

Unit tests (`-m unit`) run without Docker and are fast enough for every push.

### 1.2 No shared linting standard

Each repo manages linting independently. A shared `.pre-commit-config.yaml` in `mekong-infra` (referenced by each repo) enforces consistent standards:

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    hooks:
      - id: ruff
      - id: ruff-format
```

### 1.3 Dagster failure alerting via Slack

Phase 10 explicitly deferred Slack/email hooks. A `@run_failure_sensor` notifies immediately on asset materialisation failures rather than requiring manual UI checks:

```python
@run_failure_sensor
def slack_on_failure(context):
    message = f"Dagster run failed: {context.failure_event.message}"
    # post to Slack webhook
```

### 1.4 Consumer lag monitoring

There is no visibility into how far behind `storage_consumer` is. If it falls behind, raw data accumulates in Kafka until retention expires.

| Option | Effort | Result |
|---|---|---|
| Kafka UI (`:8080`, already running) | None | Visual lag per consumer group |
| Poll `kafka-consumer-groups.sh` + log | Low | Scriptable alerting |
| Prometheus JMX exporter + Grafana | High | Full metric history and alerting |

Kafka UI covers the immediate need. Prometheus/Grafana is the long-term target.

---

## 2. Architecture Improvements

### 2.1 Dagster asset checks — data quality as first-class assets

Phase 10 deferred asset checks. Adding `@asset_check` to `ohlcv_daily_bars` and `technical_indicators` makes quality failures visible in the Dagster UI without a separate pipeline:

```python
@asset_check(asset=ohlcv_daily_bars, blocking=True)
def ohlcv_no_negative_volume(context):
    df = spark.read.parquet("s3a://market-analysis/ohlcv.bar")
    bad = df.filter(F.col("volume") < 0).count()
    return AssetCheckResult(passed=(bad == 0), metadata={"bad_rows": bad})
```

Suggested checks:

| Asset | Check | Severity |
|---|---|---|
| `ohlcv_daily_bars` | `price <= 0` | blocking |
| `ohlcv_daily_bars` | `volume < 0` | blocking |
| `ohlcv_daily_bars` | `high < low` | blocking |
| `ohlcv_daily_bars` | `close` outside `[low, high]` | warning |
| `technical_indicators` | SMA200 null rate < 80% | warning |

### 2.2 Dagster PostgreSQL backend

`dagster.yaml` currently uses SQLite, which is fragile under concurrent daemon + webserver writes. One-line swap when the stack needs production-hardening:

```yaml
storage:
  postgres:
    postgres_url:
      env: DAGSTER_PG_URL
```

Add a `postgres` service to `docker-compose.yml` and wire `DAGSTER_PG_URL` in `.env`.

### 2.3 Raw data expiry protection via `_SUCCESS` markers

`market-data` has a 30-day lifecycle. If `ohlcv_daily_ingest` fails and raw Avro expires before it succeeds, that day's bars are unrecoverable. After a successful ingest, write a zero-byte marker:

```
s3a://market-data/_SUCCESS/year=2026/month=05/day=15
```

A Dagster `@sensor` can verify that every day with raw data has a corresponding marker and downstream OHLCV output, turning silent data loss into a visible UI alert.

### 2.4 Incremental `TechnicalJob`

`TechnicalJob` reads the entire OHLCV history on every run to compute SMA200. As data accumulates over months, this becomes proportionally more expensive. Cache the indicator state (last 200 closes per symbol) in a checkpoint Parquet file. On each run: load only new bars since the checkpoint, prepend cached history, compute indicators, update the checkpoint. Compute time stays proportional to new bars, not total history.

---

## 3. Architectural Evolution (Larger Phases)

### 3.1 Kafka Schema Registry

Kafka messages are plain JSON with no schema enforcement. A producer that renames `pct_change` to `change_pct` silently breaks all downstream consumers. Add Confluent Schema Registry and switch serialisation to Avro-over-registry. Producers register schemas on startup; consumers validate before processing.

### 3.2 Apache Iceberg table format

Replace raw Parquet in `market-analysis` with Apache Iceberg tables backed by MinIO:

- **ACID transactions** — concurrent Spark writes don't corrupt tables
- **Time travel** — `SELECT * FROM ohlcv_bars VERSION AS OF '2026-05-01'`
- **Schema evolution** — add columns without rewriting all data
- **Partition evolution** — change partitioning strategy without full rewrites

Iceberg has a REST catalog that runs as a Docker service and integrates natively with Spark.

### 3.3 Serving layer — queryable indicator API

Results live as Parquet files readable only via Spark or pandas. A lightweight API (FastAPI + DuckDB) would let other services query indicators without spinning up Spark:

```
GET /api/v1/indicators?symbol=VCB&from=2026-04-01&to=2026-05-01
→ [{date, sma20, sma50, rsi14, macd, ...}]
```

DuckDB can query S3-compatible Parquet directly with sub-second latency for single-symbol range queries.

### 3.4 Multi-exchange crypto normalisation

The crypto producer targets Binance only. A generic exchange adapter (one `CryptoProducer` class, exchange as config) would support multiple exchanges from a single container and open the door to cross-exchange spread analysis.

### 3.5 Replay / backfill from upstream APIs

If raw Avro data is lost, there is no automated recovery path. A `ReplayJob` that fetches historical OHLCV directly from vnstock/CCXT and writes to `market-analysis` would close this gap for any date range, bypassing the streaming layer.

---

## 4. Priority Table

| # | Area | Effort | Impact | Phase | Status |
|---|---|---|---|---|---|
| — | Drop `recursiveFileLookup`, partition-column filter, remove asset_class re-derivation | Medium | Low | Phase 11 | ✅ Done |
| — | Alert cooldown via Flink `MapState` per `(symbol, rule)` | Medium | High | Phase 13 | ✅ Done |
| — | Producer exponential backoff + circuit breaker | Medium | Medium | Phase 13 | ✅ Done |
| 1.1 | GitHub Actions CI across all repos | Low | High | Immediate | 📋 Planned |
| 1.2 | Shared pre-commit / ruff linting | Low | Low | Immediate | 📋 Planned |
| 1.3 | Dagster Slack failure alerting | Low | High | Phase 11 | 📋 Planned |
| 1.4 | Consumer lag monitoring | Low→High | Medium | Phase 12 | 📋 Planned |
| 2.1 | Dagster asset checks (data quality) | Medium | High | Phase 11 | 📋 Planned |
| 2.2 | Dagster PostgreSQL backend | Low | Medium | Phase 12 | 📋 Planned |
| 2.3 | Raw data expiry `_SUCCESS` markers + sensor | Medium | Medium | Phase 11 | 📋 Planned |
| 2.4 | Incremental TechnicalJob | High | Medium | Phase 14+ | 📋 Planned |
| 3.1 | Kafka Schema Registry | High | Medium | Phase 14+ | 📋 Planned |
| 3.2 | Apache Iceberg table format | High | High | Phase 15+ | 📋 Planned |
| 3.3 | Serving layer / indicator API | High | Medium | Phase 14+ | 📋 Planned |
| 3.4 | Multi-exchange crypto normalisation | Medium | Medium | Phase 14+ | 📋 Planned |
| 3.5 | Replay / backfill from upstream APIs | Medium | Medium | Phase 13 | 📋 Planned |
