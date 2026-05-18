# Improvement Suggestions

> Status: planning only. Nothing below should be coded until a phase is selected and scoped.
> Last updated: 2026-05-18 — reflects Phase 10 (Dagster) complete, Phases 11–13 planned.

---

## 1. Critical Code Fixes

These violate existing rules or produce incorrect results today.

### 1.1 `ohlcv_daily_ingest` violates the Spark reading rule

`ohlcv_daily_ingest` manually lists Avro files from MinIO and passes explicit paths to Spark:

```python
# current (wrong)
today_files = [f"s3a://..." for obj in raw_store.list_objects(...)]
df = spark.read.format("avro").load(today_files)
```

The correct approach reads the root prefix and lets Spark walk the partition tree:

```python
# correct — Spark handles partition pruning natively
df = (spark.read.format("avro")
      .load("s3a://market-data/price.snapshot")
      .filter((F.col("year") == year) & (F.col("month") == month) & (F.col("day") == day)))
```

### 1.2 `ohlcv_daily_ingest` collects to the driver and writes via PyArrow

After computing OHLCV bars the result is `.collect()`-ed to the Python driver and written back via `MinioStore.write_parquet()`. This discards Spark's distributed write capability and creates a driver-side memory bottleneck as symbol count grows.

Replace with a single native Spark write:

```python
(df_ohlcv
 .write
 .mode("overwrite")
 .partitionBy("asset_class", "year", "month", "day")
 .parquet("s3a://market-analysis/ohlcv.bar"))
```

This also makes the job idempotent — re-running the same day overwrites rather than accumulating duplicate files (see 1.5).

### 1.3 Asset class re-derived from symbol string

`ohlcv_daily_ingest` infers `asset_class` from the symbol string (`/` → crypto). But `asset_class` is already a Hive partition column in the source path. After fixing 1.1, it arrives as a column automatically — the re-derivation is redundant and fragile.

### 1.4 `LOOKBACK_DAYS` hardcoded — no backfill support

`LOOKBACK_DAYS = 0` is a module-level constant. Dagster already passes `--date` via `SparkClusterResource.submit()` but the job ignores `LOOKBACK_DAYS` in that path. Should be wired to the Dagster partition key or removed entirely in favour of reading the date from the CLI argument.

### 1.5 Duplicate Parquet files accumulate on re-runs

Running `ohlcv_daily_ingest` twice for the same day creates two files in the same partition directory. `TechnicalJob` loads both and double-counts bars. Fixed entirely by 1.2 (`mode("overwrite")`).

### 1.6 `technical_job` writes the report to the local filesystem

`reports/technical_YYYY-MM-DD.txt` is written to disk on the Spark driver — inside the `spark-master` container and invisible from the host. Should be written to MinIO so it persists across container restarts and is accessible from Jupyter:

```
s3a://market-analysis/reports/year=/month=/day=/technical.txt
```

---

## 2. Code Quality

Correctness is fine, but these reduce robustness or maintainability.

### 2.1 `StorageConsumer` — no per-row error isolation

If a single malformed message causes extraction to raise, the entire batch silently fails to flush. A try/except around per-row extraction with a dead-letter prefix (`market-data/dead-letter/`) prevents one bad message from dropping the whole batch.

### 2.2 `time` field stored as string in Avro and Parquet

Both schemas store `time` as a plain string. Parquet has first-class timestamp support — storing it as a timestamp enables partition pruning and correct range filter semantics without string parsing.

### 2.3 Flink `PriceAlertJob` runs at parallelism 1

`env.set_parallelism(1)` serialises all symbol processing onto a single thread, defeating the `key_by` partition. The TaskManager has 4 slots — setting parallelism to 2 or 4 lets symbols be processed concurrently.

### 2.4 Alert cooldown missing

`PriceAlertJob` fires on every tick while a threshold is breached. A stock at -3% across ten 30-second polls generates 10 identical alerts. Use Flink `ValueState` to store `last_fired_ts` per `(symbol, rule)` key and suppress re-fires within a configurable cooldown window (e.g. 5 minutes).

### 2.5 Producer resilience — crash on any exception

Both producers exit on unhandled exceptions from vnstock/CCXT and rely on Docker `restart: unless-stopped` to recover. There is no backoff, jitter, or distinction between transient (network timeout) and permanent (API ban) errors. Add exponential backoff with jitter and a circuit breaker that pauses the producer rather than crashing it.

### 2.6 No CI/CD across any repo

None of the six repos have GitHub Actions. Broken imports and failed tests are only discovered locally. A minimal pipeline per repo covers the gap with no infrastructure cost:

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

Unit tests (`-m unit`) run without Docker and are fast for every push. Integration tests can be gated to `main` merges.

### 2.7 No shared linting standard across repos

Each repo manages linting independently or not at all. A shared `.pre-commit-config.yaml` committed to `mekong-infra` and referenced (or symlinked) into each repo would enforce consistent standards:

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    hooks:
      - id: ruff
      - id: ruff-format
```

---

## 3. Architecture Improvements

Medium-effort changes that meaningfully improve reliability or observability.

### 3.1 Dagster asset checks — data quality as first-class assets

Phase 10 explicitly deferred asset checks. Adding `@asset_check` to `ohlcv_daily_bars` and `technical_indicators` makes quality failures visible in the Dagster UI without a separate quality pipeline:

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
| `technical_indicators` | SMA200 null rate < 80% (not enough history yet) | warning |

### 3.2 Dagster failure alerting via Slack

Phase 10 deferred Slack/email hooks. A `@run_failure_sensor` notifies immediately on asset materialisation failures rather than requiring manual UI checks:

```python
@run_failure_sensor
def slack_on_failure(context):
    message = f"Dagster run failed: {context.failure_event.message}"
    # post to Slack webhook
```

Low effort, high operational value — one of the first things to add in Phase 11.

### 3.3 Dagster PostgreSQL backend

`dagster.yaml` currently uses SQLite. SQLite is fragile under concurrent daemon + webserver writes and cannot survive a host restart cleanly in Docker volumes. One-line swap in `dagster.yaml`:

```yaml
storage:
  postgres:
    postgres_url:
      env: DAGSTER_PG_URL
```

Add a `postgres` service to `docker-compose.yml` and wire `DAGSTER_PG_URL` in `.env`.

### 3.4 Consumer lag monitoring

There is no visibility into how far behind `storage_consumer` is. If it falls behind, raw data accumulates in Kafka until retention expires. Options ranked by effort:

| Option | Effort | Result |
|---|---|---|
| Kafka UI (`:8080`, already running) | None | Visual lag per consumer group |
| Poll `kafka-consumer-groups.sh` + log | Low | Scriptable alerting |
| Prometheus JMX exporter + Grafana | High | Full metric history and alerting |

Kafka UI covers the immediate need. Prometheus/Grafana is the long-term target.

### 3.5 Raw data expiry protection via `_SUCCESS` markers

`market-data` has a 30-day lifecycle. If `ohlcv_daily_ingest` fails silently and raw Avro expires, that day's bars are unrecoverable. After a successful ingest, write a zero-byte marker:

```
s3a://market-data/_SUCCESS/year=2026/month=05/day=15
```

A Dagster `@sensor` can verify that every day with raw data has a corresponding marker and downstream OHLCV output, turning silent data loss into a visible UI alert.

### 3.6 Incremental `TechnicalJob`

`TechnicalJob` reads the entire OHLCV history on every run to compute SMA200. As months accumulate this becomes proportionally more expensive. Cache the indicator state (last 200 closes per symbol) in a checkpoint Parquet file. On each run: load only new bars since the checkpoint, prepend cached history, compute indicators, update the checkpoint. Compute time stays proportional to new bars, not total history.

---

## 4. Architectural Evolution (Larger Phases)

Bigger changes worth planning but not implementing immediately.

### 4.1 Kafka Schema Registry

Kafka messages are plain JSON with no schema enforcement. A producer that renames `pct_change` to `change_pct` silently breaks all downstream consumers. Add Confluent Schema Registry (available as a Docker image) and switch serialisation to Avro-over-registry. Producers register schemas on startup; consumers validate before processing.

### 4.2 Apache Iceberg table format

Replace raw Parquet files in `market-analysis` with Apache Iceberg tables backed by MinIO. Benefits:

- **ACID transactions** — concurrent Spark writes don't corrupt tables
- **Time travel** — `SELECT * FROM ohlcv_bars VERSION AS OF '2026-05-01'`
- **Schema evolution** — add columns without rewriting all data
- **Partition evolution** — change partitioning strategy without full rewrites

Iceberg integrates natively with Spark and has a REST catalog that runs as a Docker service. The main migration cost is a one-off backfill job to rewrite existing Parquet files.

### 4.3 Serving layer — queryable indicator API

Results currently live as Parquet files readable only via Spark or pandas. A lightweight API (FastAPI + DuckDB) would let other services query indicators without spinning up Spark:

```
GET /api/v1/indicators?symbol=VCB&from=2026-04-01&to=2026-05-01
→ [{date, sma20, sma50, rsi14, macd, ...}]
```

DuckDB can query S3-compatible Parquet directly with sub-second latency for single-symbol range queries, with no Spark cluster required.

### 4.4 Multi-exchange crypto normalisation

The crypto producer targets Binance only. Adding OKX or Bybit requires duplicating the producer loop. A generic exchange adapter pattern (one `CryptoProducer` class, exchange as config) would support multiple exchanges from a single container and open the door to cross-exchange spread analysis.

### 4.5 Replay / backfill from upstream APIs

If raw Avro data is lost (expired or corrupted), there is no automated recovery path. A `ReplayJob` that fetches historical OHLCV directly from vnstock/CCXT and writes to `market-analysis` would close this gap for any date range, bypassing the streaming layer.

---

## 5. Priority Table

| # | Area | Effort | Impact | Suggested phase |
|---|---|---|---|---|
| 1.1 | Fix Spark reading rule in `ohlcv_daily_ingest` | Low | High | Next commit |
| 1.2 | Replace driver collect + PyArrow write with Spark native write | Low | High | Next commit |
| 1.3 | Remove redundant asset_class re-derivation | Low | Low | Next commit |
| 1.4 | Wire `LOOKBACK_DAYS` to Dagster partition key | Low | Medium | Next commit |
| 1.5 | Fix duplicate Parquet via `mode("overwrite")` | Low | High | Next commit (same as 1.2) |
| 1.6 | Write technical report to MinIO | Low | Medium | Next commit |
| 2.1 | Dead-letter handling in StorageConsumer | Low | Medium | Phase 11 |
| 2.2 | `time` field as timestamp type | Medium | Medium | Phase 11 |
| 2.3 | Raise Flink parallelism beyond 1 | Low | Medium | Phase 13 |
| 2.4 | Alert cooldown / deduplication | Medium | High | Phase 13 |
| 2.5 | Producer backoff + circuit breaker | Medium | Medium | Phase 13 |
| 2.6 | GitHub Actions CI across all repos | Low | High | Immediate |
| 2.7 | Shared pre-commit / ruff linting | Low | Low | Immediate |
| 3.1 | Dagster asset checks (data quality) | Medium | High | Phase 11 |
| 3.2 | Dagster Slack failure alerting | Low | High | Phase 11 |
| 3.3 | Dagster PostgreSQL backend | Low | Medium | Phase 12 |
| 3.4 | Consumer lag monitoring | Low→High | Medium | Phase 12 |
| 3.5 | Raw data expiry `_SUCCESS` markers + sensor | Medium | Medium | Phase 11 |
| 3.6 | Incremental TechnicalJob | High | Medium | Phase 14+ |
| 4.1 | Kafka Schema Registry | High | Medium | Phase 14+ |
| 4.2 | Apache Iceberg table format | High | High | Phase 15+ |
| 4.3 | Serving layer / indicator API | High | Medium | Phase 14+ |
| 4.4 | Multi-exchange crypto normalisation | Medium | Medium | Phase 14+ |
| 4.5 | Replay / backfill from upstream APIs | Medium | Medium | Phase 13 |
