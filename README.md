# mekong-infra

Docker Compose stack and infrastructure management for the Mekong market-data platform.
No application code lives here — just infrastructure definitions and initialisation scripts.

## Assumed directory layout

Clone all Mekong repos as siblings:

```
Mekong/
  mekong-infra/        ← this repo
  mekong-jobs/
  mekong-notebooks/
  mekong-kafka/
  mekong-data-models/
```

If your repos are elsewhere, set `MEKONG_JOBS_DIR` and `MEKONG_NOTEBOOKS_DIR` in a `.env` file.

## Quick start

```bash
cp .env.example .env
make install          # interactive: choose which services to start
```

Or start everything at once:

```bash
make build            # build all Docker images (first time or after Dockerfile changes)
make up               # start full stack
make minio-init       # create buckets (run once after first MinIO start)
make topics-create    # create Kafka topics (run once after first Kafka start)
```

## Service URLs

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| MinIO console | http://localhost:9001 |
| Flink UI | http://localhost:8081 |
| Spark Master | http://localhost:8082 |
| Spark History | http://localhost:18080 |
| JupyterLab | http://localhost:8888 |
| Dagster UI | http://localhost:3000 |

## Make targets

| Target | Description |
|---|---|
| `make install` | Interactive: choose services to start + auto-init |
| `make up` | Start all services |
| `make down` | Stop all services (volumes preserved) |
| `make build` | Build all Docker images |
| `make topics-create` | Create Kafka topics (idempotent) |
| `make minio-init` | Create MinIO buckets + lifecycle rules (idempotent) |
| `make storage-flush` | Interactively delete data from MinIO buckets |
| `make flink-up` | Start Kafka + Flink only |
| `make spark-up` | Start MinIO + Spark cluster only |
| `make dagster-up` | Start MinIO + Spark + Dagster |
| `make jupyter-up` | Start MinIO + Jupyter |

## Credentials

Default credentials (`minioadmin` / `minioadmin`) work with the Docker Compose defaults.
Never commit real credentials — use `.env` for local dev and GitHub Secrets for CI.
