# Jupyter

## 1. Role

JupyterLab is the **ad-hoc exploration environment** for the data sitting in MinIO. It's a sibling to the Spark cluster, not a replacement — notebooks use `SparkFactory` in `local[*]` mode for fast iteration over small queries.

Notebooks themselves are now gitignored (the folder is bind-mounted local-only working space). The Docker image and Makefile targets stay so anyone cloning the repo can spin up the environment.

## 2. Container

| Service | Image | Ports |
|---|---|---|
| `jupyter` | custom (`docker/jupyter.Dockerfile`) | `8888:8888` (no token) |

Built from `docker/jupyter.Dockerfile`:
- Base: `python:3.12-slim`
- Adds `default-jre-headless` (PySpark JVM)
- Installs `requirements.txt` + `jupyterlab>=4.2`
- Pre-bakes S3A and Avro JARs into PySpark's local `jars/` directory — same set as the Spark image, so `local[*]` mode can read MinIO without runtime downloads
- Default working dir: `/opt/project`, default notebook dir: `/opt/project/notebooks`

## 3. Configuration

From `docker/docker-compose.yml`:

- **`PYTHONPATH: /opt/project`** — repo mounted, so `from model.spark import SparkFactory` works
- **MinIO env overrides:** `MINIO_ENDPOINT: http://minio:9000` (same logic as Spark — see `MINIO.md` §6)
- **`KAFKA_BOOTSTRAP_SERVERS: kafka:29092`** — for any notebook that wants to consume directly (rarely used)
- **`depends_on: minio (service_healthy)`** — won't start until MinIO is ready
- **Mount:** `..:/opt/project` — live repo, edits to `.py` modules in the host show up immediately in notebook kernels (after kernel restart)

JupyterLab itself starts unauthenticated (`--ServerApp.token=''`, `--ServerApp.password=''`). Acceptable for localhost dev; **do not expose this port externally**.

## 4. Operations

| Task | Command |
|---|---|
| Build image | `cd mekong-infra && make build-jupyter` (first time or after `requirements.txt` changes) |
| Start | `cd mekong-infra && make jupyter-up` → http://localhost:8888 |
| Logs | `docker logs jupyter` |
| Tear down | `cd mekong-infra && docker compose stop jupyter` |

## 5. Typical Usage

```python
# inside a notebook
from model.spark import SparkFactory

with SparkFactory("explore") as spark:
    df = spark.read.parquet("s3a://market-analysis/ohlcv.bar")
    df.groupBy("symbol").count().show()
```

Local-mode Spark is bounded by the container's RAM (no separate worker). Fine for filters, aggregations, plotting over a few days; switch to the actual Spark cluster (via `make run-spark-*`) for full-history runs.

## 6. Failure Modes & Gotchas

- **`No module named '...'`.** Notebook kernel imports failed because the host-mounted code references a Python package not in the image. Add to `requirements.txt` and rebuild.
- **Stale package after `requirements.txt` edit.** `make jupyter-build` rebuilds the image. Pure code edits on `model/`, `analysis/`, etc. don't need a rebuild — just restart the kernel.
- **S3A 403.** Same root causes as Spark — endpoint or credentials mismatch. Container uses `http://minio:9000` automatically; verify if you've manually overridden in notebook env.
- **No token but exposed on host.** The token-less config is **localhost-only safe**. If you bind to a non-localhost interface or expose 8888 publicly, anyone gets root in the container.
- **Notebooks aren't tracked.** Notebooks in `notebooks/` are gitignored on purpose. Back up anything important elsewhere — `make uninstall` (selecting Jupyter) keeps them, but `rm -rf notebooks/` doesn't.

## 7. References

- `mekong-notebooks`: `docker/jupyter.Dockerfile` (canonical), `mekong-infra`: `docker/jupyter.Dockerfile` (reference copy)
- `mekong-notebooks`: `model/spark.py` — `SparkFactory` (auto-selects local[*] in this container)
- `mekong-notebooks`: `notebooks/exploration/`, `notebooks/reporting/`
- `design/services/SPARK.md` — when to leave Jupyter for the cluster
