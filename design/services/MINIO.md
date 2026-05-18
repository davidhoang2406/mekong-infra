# MinIO

## 1. Role

MinIO is the S3-compatible object store that holds all persistent data:

- **Raw streaming data** in the `market-data` bucket (Avro, ~30 day lifecycle)
- **Derived/processed data** in the `market-analysis` bucket (Parquet, no expiry)

Spark and Flink reach it via the S3A connector; Python tooling uses the `minio` SDK directly through the `MinioStore` wrapper (`model/minio_store.py`).

## 2. Container

| Service | Image | Ports |
|---|---|---|
| `minio` | `minio/minio:RELEASE.2025-09-07T16-13-09Z` | `9000:9000` (S3 API), `9001:9001` (Web console) |

Single-node, single-volume — no erasure coding, no replication. Adequate for dev; production would run distributed.

## 3. Bucket Layout

### `market-data` — raw streaming (30-day lifecycle)

Deflate-compressed Avro, appended continuously by `consumers/storage_consumer.py`.

```
market-data/
└── price.snapshot/
    ├── asset_class=stock/
    │   └── symbol=VCB/
    │       └── year=2026/month=05/day=17/
    │           └── part-{ts_ms}.avro
    └── asset_class=crypto/
        └── symbol=BTC-USDT/
            └── year=2026/month=05/day=17/
                └── part-{ts_ms}.avro
```

### `market-analysis` — derived data (no expiry)

Spark-written Parquet partitioned by date.

```
market-analysis/
├── ohlcv.bar/asset_class={stock|crypto}/year=/month=/day=/part-*.parquet
└── technical.indicators/year=/month=/day=/part-*.parquet
```

## 4. Configuration

| Env var | Default | Used by |
|---|---|---|
| `MINIO_ROOT_USER` / `MINIO_ACCESS_KEY` | `minioadmin` | Container init, all clients |
| `MINIO_ROOT_PASSWORD` / `MINIO_SECRET_KEY` | `minioadmin` | Container init, all clients |
| `MINIO_BUCKET` | `market-data` | Storage consumer, ohlcv ingest source |
| `MINIO_ANALYSIS_BUCKET` | `market-analysis` | Spark batch jobs |

**Endpoint differs by where the client runs:**
- From the host (Python tooling, `db/init_minio.py`): `http://localhost:9000`
- From inside Docker (Spark, Flink, Jupyter): `http://minio:9000` (overridden in compose env, see §6)

## 5. Healthcheck

```
curl -f http://localhost:9000/minio/health/live
```

Used by `jupyter` and other dependents via `condition: service_healthy`.

## 6. Why Two MinIO Endpoints

Spark/Flink/Jupyter containers each have `MINIO_ENDPOINT: http://minio:9000` set explicitly in `docker-compose.yml`, **overriding** the `.env` value (`http://localhost:9000`). This is intentional:

- `.env` is for code that runs on the **host** (producers, consumers, `db/` scripts).
- Compose env vars are for code that runs **inside** the Docker network, where `localhost` would point at the container itself.

Don't try to unify these — they really do need different values.

## 7. Operations

| Task | Command |
|---|---|
| Start | `cd mekong-infra && make install` or `docker compose up -d minio` |
| Create buckets | `cd mekong-infra && make minio-init` (runs `db/init_minio.py`; idempotent) |
| Web console | http://localhost:9001 (minioadmin / minioadmin) |
| List objects | `mc ls myalias/market-data --recursive` (after `mc alias set`), or via `MinioStore.list_objects()` |
| Flush a bucket | `cd mekong-infra && make storage-flush` (interactive — asks per bucket) |
| Tear down (destroys data) | `docker compose down -v` (removes volumes) |

## 8. Lifecycle Rules

`db/init_minio.py` configures a 30-day expiry on `market-data` only — the raw stream is recoverable from upstream APIs, so it's cheap to age out. `market-analysis` has **no expiry**: derived data is expensive to recompute (Spark backfills over months of bars).

## 9. Spark Reading Rule (Critical)

**Always point Spark at the root S3A prefix; never list MinIO files manually then pass them to `read.parquet()`.** Spark walks the partition tree natively and infers partition columns. The wrong pattern bypasses partition pruning and can break with thousands of partitions.

```python
# correct
df = spark.read.parquet("s3a://market-analysis/ohlcv.bar")

# wrong — bypasses Spark's partition discovery
files = [f"s3a://market-analysis/{o.object_name}" for o in store.list_objects(...)]
df = spark.read.parquet(*files)
```

See `analysis/batch/ohlcv_daily_ingest.py` and `analysis/batch/technical_job.py` for canonical usage.

## 10. Failure Modes & Gotchas

- **403 from S3A.** Usually credentials mismatch between Spark container env and MinIO root credentials. Both must match.
- **Endpoint mismatch.** `localhost:9000` from inside a container connects to nothing. Always use `minio:9000` inside Docker.
- **Path-style addressing.** S3A defaults to virtual-hosted style (`bucket.host`), which MinIO doesn't support. The Spark/Jupyter configs in this repo set `fs.s3a.path.style.access=true` — check this if you spin up a new client.
- **Public bucket exposure.** Default policy is private; do not flip to public in this dev setup.
- **Volume bloat.** `market-data` lifecycle handles automatic cleanup, but if you disable it or run on a long backfill, the volume can grow unbounded. Monitor `docker system df`.

## 11. References

- `mekong-kafka`: `model/minio_store.py` — Avro write wrapper (ingestion side)
- `mekong-jobs`: `model/minio_store.py` — Parquet read wrapper (batch side)
- `mekong-infra`: `db/init_minio.py`, `db/flush_minio.py`
- `mekong-jobs`: `model/spark.py` — S3A connector configuration on SparkSession
- `design/DESIGN.md` §5a (storage schema)
