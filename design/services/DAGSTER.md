# Phase 10 — Orchestration (Dagster)

**Status:** Implemented ✅ · Lives in `mekong-jobs/dagster/`
**Author:** David Hoang
**Date:** 2026-05-16

---

## 1. Problem

Phases 1–9 left the pipeline with two distinct workload types:

| Type | Examples | How it runs today |
|---|---|---|
| **Continuous services** | Stock/crypto producers, storage consumer, alert consumer, Flink alert job | `make run-*` in separate terminals (or Docker containers) — run forever, no orchestration needed |
| **Batch jobs** | `ohlcv_daily_ingest`, `technical_job`, future `digest_job`, `screener_job` | Manually invoked via `make` — no schedule, no dependency tracking, no backfill UI |

The batch side has implicit dependencies (`technical_job` and `digest_job` need today's OHLCV bars to exist) and benefits from features no Makefile can give us: scheduling, retries, partition-aware backfill, lineage UI, failure alerts.

**Phase 10 introduces an orchestrator for the batch workloads only. Streaming services are out of scope and stay in Docker Compose.**

---

## 2. Why Dagster

Considered Airflow, Dagster, Prefect, Mage, Argo. Picked Dagster because:

1. **Asset-centric model maps onto our storage layout.** The MinIO directory structure already *is* a graph of data assets:

   ```
   price.snapshot  →  ohlcv.bar  →  technical.indicators
                                 ↘  digest
                                 ↘  screener
   ```

   Dagster makes those nouns first-class via `@asset`. Airflow's task-centric DAG would model the same flow but lose the lineage semantics for free.

2. **Daily partitions match `year=/month=/day=/` natively.** `DailyPartitionsDefinition` and the partition-aware backfill UI line up with how the data is already physically laid out — no impedance.

3. **Pytest-friendly.** Asset functions are plain Python; we can unit-test them without spinning up a scheduler. Aligns with the existing `tests/` setup.

4. **Lower infra footprint than Airflow.** A single `dagster-webserver` + `dagster-daemon` pair plus a SQLite (dev) / Postgres (later) store. Fits the local-Docker ethos of this repo.

The trade-off we accept: smaller community, fewer Spark-specific helpers. Mitigated by treating Spark as an *external compute target* (see §6) rather than asking Dagster to manage the cluster.

---

## 3. Asset Graph

```
        ┌──────────────────────────┐
        │ price_snapshots          │  ← source asset (MinIO market-data)
        │ (external, daily parts)  │     produced by StorageConsumer
        └──────────────┬───────────┘
                       │
                       ▼
        ┌──────────────────────────┐
        │ ohlcv_daily_bars         │  daily-partitioned
        │ (MinIO market-analysis)  │  computed by ohlcv_daily_ingest Spark job
        └──┬───────────────────┬───┘
           │                   │
           ▼                   ▼
   ┌──────────────────┐  ┌──────────────────────┐
   │ technical_       │  │ daily_digest         │  daily-partitioned
   │ indicators       │  │ (Phase 11)           │
   │ (Parquet,        │  │                      │
   │  daily-part'd)   │  └──────────────────────┘
   └──────────────────┘

   ┌──────────────────────────┐
   │ screener_results         │  weekly-partitioned, independent of OHLCV
   │ (Phase 11)               │     (fundamentals don't change daily)
   └──────────────────────────┘
```

| Asset | Partition | Upstream | Compute | Output |
|---|---|---|---|---|
| `price_snapshots` | daily (Y/M/D) | — | StorageConsumer (out of orchestrator) | `s3a://market-data/price.snapshot/...avro` |
| `ohlcv_daily_bars` | daily (Y/M/D) | `price_snapshots` | Spark — `ohlcv_daily_ingest` | `s3a://market-analysis/ohlcv.bar/...parquet` |
| `technical_indicators` | daily (Y/M/D) | `ohlcv_daily_bars` (window of last 200 days) | Spark — `technical_job` | `s3a://market-analysis/technical.indicators/...parquet` |
| `daily_digest` *(Phase 11)* | daily | `ohlcv_daily_bars` | Spark — TBD | `s3a://market-analysis/digest/...parquet` |
| `screener_results` *(Phase 11)* | weekly | — (vnstock fundamentals) | Spark — TBD | `s3a://market-analysis/screener/...parquet` |

`price_snapshots` is modelled as a **source asset** (`SourceAsset` or `@observable_source_asset`) because it's produced by something outside Dagster. We just observe it for freshness — we never materialise it.

`technical_indicators` is daily-partitioned but the Spark job reads the *full history* to compute SMA200 etc. That's a "fan-in" window — partition X depends on partitions [X-200, X]. Dagster supports this via `AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-200))`.

---

## 4. Partitioning Strategy

```python
from dagster import DailyPartitionsDefinition

daily_partitions = DailyPartitionsDefinition(
    start_date="2026-05-01",   # earliest day we have price.snapshot data for
    timezone="Asia/Ho_Chi_Minh",
)
```

- **One partition = one trading day.** Aligns 1:1 with the `year=/month=/day=` MinIO path and with the existing `DATE=YYYY-MM-DD` backfill arg in `make run-ohlcv-daily-ingest`.
- **Timezone:** HOSE trades in Asia/Ho_Chi_Minh; daily partitions snap to local midnight, which means "today" in the UI matches "today" on the exchange.
- **Crypto is 24/7** — same daily partition works, no special-casing.

---

## 5. Resources

```python
# dagster_project/resources.py

class MinioResource(ConfigurableResource):
    endpoint: str
    access_key: str
    secret_key: str
    market_data_bucket: str
    market_analysis_bucket: str

class SparkClusterResource(ConfigurableResource):
    """Submits jobs to the existing Docker Spark cluster via `docker exec spark-master`."""
    master_url: str = "spark://spark-master:7077"
    container_name: str = "spark-master"

    def submit(self, main_module: str, args: list[str]) -> None:
        # equivalent to the existing `make run-spark-*` recipes
        ...
```

All config values come from `.env` (already-committed). No new env vars introduced.

---

## 6. Spark Integration — Treat Spark as External Compute

**Key decision:** keep `analysis/batch/*.py` and `main.py` unchanged. Dagster assets are thin wrappers that call the existing CLI entry points.

```python
@asset(partitions_def=daily_partitions, deps=[price_snapshots])
def ohlcv_daily_bars(context, spark: SparkClusterResource):
    target_date = context.partition_key   # e.g. "2026-05-16"
    spark.submit(
        main_module="main.py",
        args=["ohlcv-daily-ingest", "--date", target_date],
    )
```

Under the hood this is the same `docker exec spark-master spark-submit ...` that the Makefile uses today. The Spark code stays in the spark-master container; Dagster only orchestrates from outside.

**Why not `dagster-pyspark`?** That library is designed for in-process PySpark, which would mean rebuilding our Spark image to include Dagster, or running Spark in local mode inside the Dagster container. Both fight the existing architecture. The subprocess-to-existing-cluster pattern is the path of least disruption and is how most real-world Dagster + Spark shops integrate.

---

## 7. Sensors & Schedules

| Trigger | Type | What it does |
|---|---|---|
| `daily_market_close` | `@schedule` (cron `0 16 * * 1-5` Asia/Ho_Chi_Minh) | Kicks off `ohlcv_daily_bars` for *today*'s partition right after HOSE close (15:00 + 1h buffer) |
| `price_snapshots_freshness` | `@observable_source_asset` | Checks the latest `year=/month=/day=` partition in MinIO every 15 min; Dagster's freshness UI shows red if stale |
| `weekly_screener` *(Phase 11)* | `@schedule` (cron `0 8 * * 1`) | Monday-morning fundamentals refresh |

`technical_indicators` and `daily_digest` are *not* directly scheduled — they auto-materialise when their upstream (`ohlcv_daily_bars`) becomes available, via Dagster's auto-materialisation policy.

---

## 8. Project Structure (implemented)

```
mekong-dagster/                         # orchestration repo
├── pyproject.toml
├── dagster.yaml                        # SQLite instance config
├── workspace.yaml                      # points to dagster_project package
├── dagster_project/
│   ├── __init__.py                     # exports `defs: Definitions`
│   ├── assets/
│   │   ├── price_snapshots.py          # @observable_source_asset
│   │   ├── ohlcv.py                    # @asset ohlcv_daily_bars
│   │   └── technical.py               # @asset technical_indicators
│   ├── resources.py                    # MinioResource, SparkClusterResource
│   ├── schedules.py                    # daily_market_close (0 16 * * 1-5 HCM)
│   └── partitions.py                  # DailyPartitionsDefinition
└── tests/
    └── test_assets.py

mekong-infra/
├── docker/dagster.Dockerfile           # webserver + daemon image
└── docker-compose.yml                  # dagster-webserver + dagster-daemon services
```

Dagster runs as **two long-lived containers**:
- `dagster-webserver` — UI on :3000
- `dagster-daemon` — handles schedules, sensors, run queue

Backed by SQLite for now (single-node, ephemeral). Postgres swap is a one-line `dagster.yaml` change when we outgrow it.

---

## 9. Local Development Flow

```bash
# One-time
cd mekong-infra && make build-dagster  # build dagster.Dockerfile
cd mekong-dagster && make dagster-up   # start webserver + daemon → http://localhost:3000

# Day-to-day
# - Edit assets in mekong-dagster/dagster_project/
# - Hot reload: webserver re-imports on file change
# - Manually trigger a partition from the UI for a quick smoke test
# - Backfill any historical date range from the UI's Backfills tab
```

Existing `make run-*` targets keep working — they're still useful for ad-hoc invocations and CI smoke tests.

---

## 10. Failure Handling

| Failure mode | Strategy |
|---|---|
| Spark job exits non-zero | `RetryPolicy(max_retries=2, delay=300)` on the asset — retries the partition after 5 min |
| MinIO unreachable | Resource init fails → asset run fails fast, surfaced in UI |
| Stale upstream (no `price_snapshots` for the day) | Freshness check on the observable source asset turns red in the UI; auto-materialise is gated on freshness so downstream won't fire |
| Partial parquet write | The Spark job uses `.write.mode("overwrite")` — re-running the partition cleanly replaces it |

Alerts: Phase 10 ships with **UI-only** failure visibility. Slack/email hooks via `@run_failure_sensor` are deferred to a follow-up — not blocking.

---

## 11. Out of Scope (this phase)

- Streaming services (producers, consumers, Flink job) — **stay in Docker Compose**, not orchestrated by Dagster. Dagster's `@observable_source_asset` is the only touchpoint, and only to surface freshness.
- Multi-tenant deployments / Dagster Cloud
- Postgres-backed Dagster instance (SQLite is fine for now)
- Slack/email failure notifications
- Cross-cluster Spark (only the existing Docker `spark-master` is targeted)
- Asset checks (data quality assertions) — clean follow-up phase

---

## 12. Rollout Plan

| Step | What | Test |
|---|---|---|
| 1 | Scaffold `orchestration/` project; add `dagster.Dockerfile`; `make orchestration-up` boots an empty webserver | UI loads at :3000 |
| 2 | Add `price_snapshots` source asset; observe freshness | Freshness shows current day's partition |
| 3 | Add `ohlcv_daily_bars` asset wrapping existing Spark job; trigger one partition manually | New partition appears in `s3a://market-analysis/ohlcv.bar/...` |
| 4 | Add `technical_indicators` asset with 200-day window mapping; trigger one partition | New partition appears in `technical.indicators/` |
| 5 | Add daily schedule + auto-materialise for downstream | Schedule next day triggers full chain end-to-end |
| 6 | Run backfill for 2026-05-01 → today via UI | All historical partitions materialise without errors |
| 7 | Document in README + update DESIGN.md architecture diagram | PR ready to merge |

Each step is a separate commit on the Phase 10 feature branch; the whole thing ships as one PR per the project's "one PR per phase" rule.

---

## 13. Open Questions

1. **Single Dagster image or separate webserver/daemon images?** Official docs prefer one image with different `command:` per container — going with that unless we hit a reason not to.
2. **Where does `dagster.yaml` (instance config) live?** Inside `orchestration/` and bind-mounted into both containers. SQLite store at `/opt/dagster/dagster_home/storage/`.
3. **Do we need a Dagster-side MinIO sensor *and* a source asset?** Probably just the source asset for now — the observation auto-detects new partitions. Sensors are deferred.
4. **Should the Spark resource use `docker exec` or `spark-submit --master spark://spark-master:7077` directly from the Dagster container?** First option is what `make` already does and avoids networking the Dagster container into the Spark cluster. Going with `docker exec` initially.
5. **Asset versioning / re-materialisation policy?** Default for now — explicit re-runs only. Auto-materialise policies can be tuned after seeing real failure modes.

---

## 14. References

- Dagster docs: https://docs.dagster.io/
- `DailyPartitionsDefinition`: https://docs.dagster.io/_apidocs/partitions
- `TimeWindowPartitionMapping`: for the 200-day window in `technical_indicators`
- Existing pipeline design: `design/DESIGN.md`
