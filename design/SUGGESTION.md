# Improvement Suggestions — Planning Phase

> Status: planning only. Nothing below should be coded until a phase is selected and scoped.

---

## 1. Critical Code Fixes

These violate existing rules or produce incorrect results today.

### 1.1 `ohlcv_daily_ingest.py` violates the Spark reading rule

`ohlcv_daily_ingest` manually lists Avro files from MinIO and passes explicit paths to Spark:

```python
# current (violates CLAUDE.md rule)
today_files = [f"s3a://..." for obj in raw_store.list_objects(...)]
df = spark.read.format("avro").load(today_files)
```

The correct approach is to read the root prefix and let Spark walk the partition tree, then filter by date partition columns:

```python
# correct
df = (spark.read.format("avro")
      .load("s3a://market-data/price.snapshot")
      .filter((F.col("year") == year) & (F.col("month") == month) & (F.col("day") == day)))
```

This also means Spark will infer `asset_class`, `symbol`, `year`, `month`, `day` as partition columns automatically, making the separate asset-class inference step unnecessary (see 1.3).

### 1.2 `ohlcv_daily_ingest.py` collects to the driver and writes via PyArrow

After Spark computes OHLCV bars, the result is collected to the Python driver and written back to MinIO via `MinioStore.write_parquet()`. This discards Spark's distributed write capability:

```python
# current — bottleneck on the driver
for row in df_ohlcv.collect():
    class_bars[row["asset_class"]].append(...)
analysis_store.write_parquet(key, OHLCV_BAR_SCHEMA, bars)
```

The correct approach is a single Spark write partitioned by asset class:

```python
# correct — Spark writes in parallel, partitions maintained
(df_ohlcv
 .write
 .mode("overwrite")
 .partitionBy("asset_class", "year", "month", "day")
 .parquet("s3a://market-analysis/ohlcv.bar"))
```

This also makes the job idempotent (re-running the same day overwrites rather than appending duplicate files).

### 1.3 Asset-class is re-derived from symbol instead of using the partition column

`ohlcv_daily_ingest` infers `asset_class` from the symbol string (`/` → crypto):

```python
F.when(F.col("symbol").contains("/"), "crypto").otherwise("stock")
```

But `asset_class` is already a Hive partition column in the source path. Once the Spark reading rule is applied (1.1), it comes through automatically as a column. Re-deriving it from the symbol is fragile and redundant.

### 1.4 `ohlcv_daily_ingest.py` — `LOOKBACK_DAYS` hardcoded, no backfill support

`LOOKBACK_DAYS = 0` is a module-level constant. Backfilling a missed day requires editing the source file. Should be read from an env var or a CLI argument so `make run-ohlcv-daily-ingest DATE=2026-05-10` is possible without code changes.

### 1.5 Duplicate Parquet files accumulate on re-runs

Running `ohlcv_daily_ingest` twice for the same day produces two files:

```
ohlcv.bar/asset_class=stock/year=2026/month=05/day=15/part-1715xxx.parquet
ohlcv.bar/asset_class=stock/year=2026/month=05/day=15/part-1715yyy.parquet
```

`TechnicalJob` then loads both and double-counts bars. Using `write.mode("overwrite").partitionBy(...)` (see 1.2) eliminates this entirely.

---

## 2. Code Quality Improvements

Correctness is fine, but these reduce robustness or maintainability.

### 2.1 `StorageConsumer` — no per-row error isolation

If a single malformed message causes `_EXTRACTORS[event_type](msg)` to raise, the entire batch silently fails to flush. A try/except around per-row extraction with a dead-letter path (e.g. a `price.snapshot.dlq` topic or a `dead-letter/` MinIO prefix) would prevent one bad message from dropping a whole batch.

### 2.2 Alert rule evaluation duplication — ✅ resolved

Previously `_check()` in `alert_consumer.py` and `PriceAlertFunction.process_element()` in `price_alert_job.py` implemented the same rule-matching loop. The shared logic now lives in `producers/utils.py::evaluate_rules`, and `consumers/alert_consumer.py` has been removed — `PriceAlertJob` is the only remaining alerter.

### 2.3 `SparkFactory` missing performance config

`SparkFactory._build()` doesn't set adaptive query execution or the shuffle partition count. For a small 1-worker cluster (2 cores), the Spark defaults are poorly sized:

```python
.config("spark.sql.adaptive.enabled",        "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
.config("spark.sql.shuffle.partitions",      "8")   # scale with worker cores
```

### 2.4 `technical_job.py` writes the report to the local filesystem

`reports/technical_YYYY-MM-DD.txt` is written to disk on whatever node runs the driver. In Docker cluster mode that is the `spark-master` container — the file ends up inside the container and is invisible from the host unless the volume is mounted. The report should be written to MinIO (`s3a://market-analysis/reports/`) so it is accessible from Jupyter and persists across container restarts.

### 2.5 `Flink PriceAlertJob` runs at parallelism=1

`env.set_parallelism(1)` serialises all symbol processing onto a single thread, defeating the `key_by` partition. The TaskManager has 4 task slots. Setting parallelism to 2 or 4 would let multiple symbols be processed in parallel.

### 2.6 `time` field stored as string in both Avro and Parquet schemas

Both `PRICE_SNAPSHOT_AVRO_SCHEMA` and `OHLCV_BAR_SCHEMA` store `time` as a plain string. Parquet has first-class `timestamp` support — storing it as a timestamp enables partition pruning and range filters on time columns without string parsing, and makes Jupyter/Spark queries like `filter(col("time") > "2026-01-01")` work correctly with proper type semantics.

---

## 3. Architecture Improvements

Medium-effort changes that meaningfully improve the system's reliability or scalability.

### 3.1 Add an orchestration layer (Airflow / Prefect)

Currently `ohlcv_daily_ingest` and `technical_job` are manually triggered via `make`. There is no:
- Scheduling (run at market close)
- Dependency enforcement (`technical_job` must not run before `ohlcv_daily_ingest` succeeds)
- Retry on failure
- Run history / audit trail

**Suggestion:** Add Apache Airflow as a Docker service. A single DAG with two tasks captures the dependency:

```
ohlcv_daily_ingest >> technical_job
```

Airflow also enables scheduled backfill (`airflow dags backfill`) — covering the same problem as 1.4 without any producer-side change.

The new `astronomer-data` skills available in this session make this a well-supported next step.

### 3.2 Add a data quality gate between ingest stages

There is currently no validation between Kafka → MinIO or MinIO → OHLCV. Bad data flows silently into `market-analysis`:
- Price = 0 (caught in alerts.json but not blocked from storage)
- Negative volume
- Missing fields

**Suggestion:** After `ohlcv_daily_ingest` aggregates bars, run a lightweight quality check before the write:

| Check | Threshold | Severity |
|---|---|---|
| `price <= 0` | 0 rows | critical |
| `volume < 0` | 0 rows | critical |
| `high < low` | 0 rows | critical |
| `close` outside `[low, high]` | 0 rows | warning |
| symbols with < 2 ticks | — | warning (log only) |

Fail the job on any critical check; log warnings without blocking.

### 3.3 Alert cooldown / deduplication

`PriceAlertJob` fires on every tick while a threshold is breached. A stock holding at -3% across ten 30-second polls generates 10 identical alerts.

**Suggestion:** Use Flink `ValueState` to persist `last_fired_ts` per (symbol, rule) key and suppress re-fires within a configurable cooldown window (e.g. 5 minutes). This is the natural next step from the planned `VolatilityBurstJob` (Phase 13) and teaches per-key state management in a real use case.

### 3.4 Protect raw data expiry with a pipeline completion marker

`market-data` has a 30-day lifecycle. If `ohlcv_daily_ingest` fails for a given day and the raw Avro expires, that day's bars are unrecoverable. 

**Suggestion:** After a successful ingest, write a zero-byte marker object:

```
market-data/_SUCCESS/year=2026/month=05/day=15
```

Apply the 30-day lifecycle only to `price.snapshot/` prefixes. Before expiry, a monitoring job can verify that every day with raw data has a corresponding `_SUCCESS` marker and OHLCV output — alerting if any gap is detected.

### 3.5 Add a serving layer for reports

`technical_job` outputs a `.txt` file. `DigestJob` and `ScreenerJob` (planned) will likely do the same. This makes the results:
- Not queryable (can't ask "what was VCB's RSI on 2026-04-01?")
- Not accessible to Jupyter without reading raw text
- Ephemeral in Docker cluster mode (see 2.4)

**Suggestion:** Write indicator results as Parquet to a `market-serving` MinIO bucket alongside the text report:

```
market-serving/
└── technical.indicators/
    └── year=2026/month=05/day=15/part-0.parquet
```

Jupyter can then do cross-day queries like "plot RSI14 for VCB over 60 days" with a single Spark or pandas read.

---

## 4. Architectural Evolution (Larger Phases)

These are bigger changes worth planning but not implementing immediately.

### 4.1 Kafka Schema Registry

Currently Kafka messages are plain JSON with no schema enforcement. A producer that renames `pct_change` to `change_pct` silently breaks all downstream consumers.

**Suggestion:** Add Confluent Schema Registry (available as a Docker image) and switch Kafka serialisation from JSON to Avro-over-registry. Producers register schemas on startup; consumers validate before processing. This also enables schema evolution tracking.

### 4.2 Consumer lag monitoring

There is no visibility into how far behind `storage_consumer` is. If it falls behind (e.g. vnstock API spike), raw data sits unwritten in Kafka until retention expires.

**Suggestion:** Expose consumer group lag as a metric. The simplest path is polling `kafka-consumer-groups.sh --describe` periodically and logging the lag. A more complete approach adds a Prometheus JMX exporter sidecar to the Kafka container and a Grafana dashboard (Kafka UI already shows lag, but there are no alerts on it).

### 4.3 Incremental `TechnicalJob` computation

`technical_job` reads every OHLCV bar ever written, applies full window computations, and discards everything except the last row per symbol. As data accumulates over months, this gets proportionally more expensive.

**Suggestion:** Cache the indicator state (e.g. last 200 closes per symbol) in a checkpoint Parquet file in `market-serving`. On each run, load only new bars since the last checkpoint, prepend the cached history, compute indicators, and update the checkpoint. This keeps compute time proportional to new bars rather than total history.

### 4.4 Multi-source producer resilience

Both producers crash on any unhandled exception from vnstock/CCXT and rely on Docker restart policies to recover. There is no circuit breaker, jitter in the retry interval, or distinction between transient errors (network timeout) and permanent errors (API rate limit ban).

**Suggestion:** Add exponential backoff with jitter to the retry loop, a max-retry circuit breaker that pauses the producer rather than crashing it, and structured error logging with error type classification (`transient` / `permanent`).

---

## 5. Summary Priority Table

| # | Area | Effort | Impact | Suggested Phase |
|---|---|---|---|---|
| 1.1 | Fix Spark reading rule in `ohlcv_daily_ingest` | Low | High | Next commit |
| 1.2 | Replace driver collect + PyArrow write with Spark native write | Low | High | Next commit |
| 1.3 | Remove redundant asset_class re-derivation | Low | Low | Next commit |
| 1.4 | Parameterise `LOOKBACK_DAYS` as env var / CLI arg | Low | Medium | Next commit |
| 1.5 | Fix duplicate Parquet files via `mode("overwrite")` | Low | High | Next commit (same as 1.2) |
| 2.1 | Dead-letter handling in StorageConsumer | Low | Medium | Phase 10 |
| 2.2 | Deduplicate alert rule logic | Low | Low | Phase 10 |
| 2.3 | Add AQE + shuffle partition config to SparkFactory | Low | Medium | Phase 10 |
| 2.4 | Write technical report to MinIO | Low | Medium | Phase 10 |
| 2.5 | Raise Flink parallelism beyond 1 | Low | Medium | Phase 12 |
| 2.6 | `time` field as timestamp type in Parquet | Medium | Medium | Phase 10 |
| 3.1 | Orchestration with Airflow | High | High | Phase 13 |
| 3.2 | Data quality gate | Medium | High | Phase 10 |
| 3.3 | Alert cooldown / deduplication | Medium | High | Phase 12 |
| 3.4 | Raw data expiry protection via `_SUCCESS` marker | Medium | Medium | Phase 11 |
| 3.5 | Serving layer (Parquet output for indicators) | Medium | High | Phase 11 |
| 4.1 | Schema registry | High | Medium | Phase 14+ |
| 4.2 | Consumer lag monitoring | Medium | Medium | Phase 13 |
| 4.3 | Incremental TechnicalJob | High | Medium | Phase 14+ |
| 4.4 | Producer resilience (backoff + circuit breaker) | Medium | Medium | Phase 13 |
