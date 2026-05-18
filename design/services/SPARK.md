# Spark

## 1. Role

Apache Spark runs the **batch jobs** that derive analytical data from the raw price stream:

| Job | File (in `mekong-jobs`) | Reads | Writes |
|---|---|---|---|
| `ohlcv_daily_ingest` | `jobs/batch/ohlcv_daily_ingest.py` | `s3a://market-data/price.snapshot/...avro` (one day) | `s3a://market-analysis/ohlcv.bar/...parquet` |
| `technical_job` | `jobs/batch/technical_job.py` | `s3a://market-analysis/ohlcv.bar/...` (full history) | `s3a://market-analysis/technical.indicators/...parquet` |

A standalone Docker cluster is used (not local mode) so jobs mirror a production submission flow and the Spark History Server can record runs.

## 2. Containers

| Service | Image | Ports | Role |
|---|---|---|---|
| `spark-master` | custom (`docker/spark.Dockerfile`) | `7077:7077` (submit), `8082:8080` (Web UI) | Cluster coordinator; jobs are submitted here |
| `spark-worker` | same custom image | — | Executor (`SPARK_WORKER_MEMORY: 2g`, `SPARK_WORKER_CORES: 2`) |
| `spark-history-server` | same custom image | `18080:18080` (UI) | Reads event logs from the shared `spark_logs` volume to replay completed runs |

All three share `docker/spark.Dockerfile`:
- Base: `apache/spark:4.1.1`
- Adds Python deps: `fastavro`, `pyarrow>=16`, `minio>=7.2`, `python-dotenv`, `pandas>=2.2.0`, `numpy>=1.26`
- Pre-bakes JARs into `/opt/spark/jars/`: `hadoop-aws-3.4.1.jar`, `aws-sdk-bundle-2.24.6.jar`, `spark-avro_2.13-4.1.1.jar`

## 3. Why Pre-Baked JARs

S3A connector + Avro reader are required at job runtime. Pre-baking avoids `--packages` Ivy downloads (which fail in air-gapped or slow-network environments). The pandas pin **must** be `>=2.2.0` to match PySpark 4.1.1's `applyInPandas` requirement — looser pins cause `PACKAGE_NOT_INSTALLED` at runtime.

Hadoop-aws JAR version **must match the bundled Hadoop major.minor** (Spark 4.1.1 ships Hadoop 3.4.1). Mismatched versions cause cryptic errors like `NumberFormatException` on duration strings such as `"60s"`.

## 4. Configuration

From `docker/docker-compose.yml`:

- **`PYTHONPATH: /opt/project`** — repo mounted at `/opt/project`; PySpark workers resolve `from model.spark import SparkFactory` etc.
- **`SPARK_MASTER_URL: spark://spark-master:7077`** — read by `SparkFactory` to skip local mode.
- **MinIO env overrides:** `MINIO_ENDPOINT: http://minio:9000` overrides the host-facing `.env` value. See `MINIO.md` §6 for why both exist.
- **`spark_logs` volume** is mounted into master and history-server so completed jobs show up in the History Server UI.

## 5. SparkFactory

`model/spark.py` is the single entry point — never call `SparkSession.builder` directly. Context-manager interface:

```python
from model.spark import SparkFactory

with SparkFactory("MyJob") as spark:
    df = spark.read.parquet("s3a://market-analysis/ohlcv.bar")
    ...
```

It auto-selects master:
- `SPARK_MASTER_URL` set (compose env or `.env`) → cluster
- Unset → `local[*]` (Jupyter and tests)

And pre-configures S3A (endpoint, path-style addressing, credentials). Local mode and cluster mode use identical job code as a result.

## 6. Submitting a Job

```bash
cd mekong-jobs
make run-ohlcv-daily-ingest                 # today
make run-ohlcv-daily-ingest DATE=2026-05-10 # backfill
make run-spark-technical                    # technical indicators
```

These expand to:

```bash
docker exec spark-master bash -c '\
  PYTHONPATH=/opt/project /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf "spark.executorEnv.PYTHONPATH=/opt/project" \
    --conf "spark.executorEnv.MINIO_ENDPOINT=$MINIO_ENDPOINT" \
    --conf "spark.executorEnv.MINIO_ACCESS_KEY=$MINIO_ACCESS_KEY" \
    --conf "spark.executorEnv.MINIO_SECRET_KEY=$MINIO_SECRET_KEY" \
  /opt/project/main.py ohlcv-daily-ingest --date 2026-05-10'
```

`spark.executorEnv.*` propagates MinIO creds from master env into worker JVMs — without these, executors fail S3A auth even if the driver succeeds.

## 7. The Spark Reading Rule

**Always read partitioned data from the root S3A prefix.** Spark walks the partition tree itself and infers `asset_class`, `symbol`, `year`, `month`, `day` as columns. Never list MinIO files manually and pass paths into `read.parquet()` — that pattern bypasses partition pruning and breaks on large prefixes.

See `CLAUDE.md` and `design/services/MINIO.md` §9 for the canonical example.

## 8. UIs

| URL | What it shows |
|---|---|
| http://localhost:8082 | Spark Master Web UI — live workers, running apps, completed apps (only while master is up) |
| http://localhost:18080 | Spark History Server — persisted job event logs from `spark_logs` volume; survives master restarts |

## 9. Operations

| Task | Command |
|---|---|
| Build image | `cd mekong-infra && make build-spark` |
| Start cluster | `cd mekong-infra && make spark-up` |
| Submit OHLCV ingest | `cd mekong-jobs && make run-ohlcv-daily-ingest [DATE=YYYY-MM-DD]` |
| Submit Technical job | `cd mekong-jobs && make run-spark-technical` |
| Logs | `docker logs spark-master` / `docker logs spark-worker` |
| Tear down | `cd mekong-infra && docker compose stop spark-master spark-worker spark-history-server` |

## 10. Failure Modes & Gotchas

- **`PACKAGE_NOT_INSTALLED: Pandas >= 2.2.0`.** The image hasn't been rebuilt since the pandas pin was bumped. Run `make spark-build` and recreate containers.
- **403 from S3A.** Worker JVMs didn't receive MinIO creds. Make sure the spark-submit command passes `spark.executorEnv.MINIO_*` (the Makefile recipes already do).
- **`NumberFormatException` parsing `"60s"`.** Hadoop-aws JAR version doesn't match the bundled Hadoop version. Pin both together when upgrading Spark.
- **Empty output for `technical_job`.** With <14 days of OHLCV history per symbol, every indicator is null by design (see the `_MIN` guard in `analysis/batch/technical_job.py`). Backfill more days of `ohlcv_daily_ingest` first.
- **Stale repo mount.** Code in containers is **bind-mounted** from the host — edits take effect immediately, no rebuild. But the JARs and Python deps come from the image: bump those = rebuild.
- **Driver runs inside master container.** `docker exec spark-master spark-submit` means the driver process lives on the master. The 2 GB worker memory limit doesn't apply to it.

## 11. References

- `mekong-jobs`: `model/spark.py` — `SparkFactory`
- `mekong-jobs`: `jobs/batch/ohlcv_daily_ingest.py`, `jobs/batch/technical_job.py`
- `mekong-infra`: `docker/spark.Dockerfile`
- `design/services/DAGSTER.md` — Dagster orchestrates these jobs via `SparkClusterResource`
- `design/DESIGN.md` §5 (job design rationale)
