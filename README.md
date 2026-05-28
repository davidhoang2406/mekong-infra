# mekong-infra

Kubernetes manifests, Helm values, and bootstrap tooling for the Mekong
market-data platform. No application code lives here — only infrastructure
definitions and initialisation jobs.

Earlier Docker Compose layout has been removed; everything now runs on K8s.

## Repository layout

```
k8s/
  mekong-data/           # Kafka (StatefulSet), MinIO, Schema Registry, Kafka UI
  mekong-pipeline/       # producers (stock/crypto) + storage consumer
  mekong-processing/     # Flink + Spark History Server (uses operator CRDs)
  mekong-orchestration/  # Dagster daemon, webserver, Postgres
  mekong-dev/            # JupyterLab
  mekong-observability/  # Loki stack values
  rbac/                  # ServiceAccount + Role + RoleBinding per namespace
  secrets/               # placeholder secret manifests (real values gitignored)
  ingress.yaml           # *.mekong.local routes
config/                  # symbol lists, alert thresholds (mounted via ConfigMaps)
design/                  # architecture docs + service deep-dives
Makefile                 # k8s-* targets — see below
```

## Prerequisites

- A running Kubernetes cluster (minikube, kind, k3d, or a real cluster).
- `kubectl` and `helm` on your PATH.
- For local dev: `minikube` with the `ingress` and `metrics-server` addons.

```bash
minikube start --cpus=8 --memory=12g --driver=docker
minikube addons enable ingress
```

## Bootstrap (first time)

```bash
make k8s-operators        # install Flink + Spark Helm operators in mekong-processing
make k8s-namespaces       # create all namespaces
# Edit k8s/secrets/*.yaml with real values, then:
make k8s-secrets          # apply + mirror MinIO/Telegram secrets cross-namespace
make k8s-rbac             # apply RBAC
make k8s-up               # deploy data + pipeline + processing + dagster + dev + obs
make k8s-topics-create    # one-shot K8s Job to create Kafka topics
make k8s-minio-init       # one-shot K8s Job to create MinIO buckets
```

## Make targets

| Target | Purpose |
|---|---|
| `make k8s-operators` | Install Flink + Spark Kubernetes operators (run once) |
| `make k8s-namespaces` | Create all `mekong-*` namespaces |
| `make k8s-secrets` | Apply local secret manifests + mirror them across namespaces |
| `make k8s-rbac` | Apply ServiceAccount + Role + RoleBinding manifests |
| `make k8s-data-up` | Deploy Kafka, MinIO, Schema Registry, Kafka UI |
| `make k8s-pipeline-up` | Deploy producers + storage consumer |
| `make k8s-processing-up` | Deploy Flink + Spark History Server |
| `make k8s-dagster-up` | Deploy Postgres + Dagster webserver/daemon |
| `make k8s-logging-up` | Install Loki stack via Helm |
| `make k8s-dev-up` | Deploy JupyterLab |
| `make k8s-topics-create` | Create Kafka topics via one-shot Job |
| `make k8s-minio-init` | Create MinIO buckets + lifecycle rules via one-shot Job |
| `make k8s-up` | Bring up the entire stack |
| `make k8s-down` | Delete all workloads (PVCs preserved — data survives) |
| `make k8s-status` | Show pod status across all namespaces |

## Service URLs

Ingress hosts (add to `/etc/hosts` pointing at `minikube ip`):

| Service | Host |
|---|---|
| Kafka UI | `kafka-ui.mekong.local` |
| MinIO console | `minio.mekong.local` |
| Schema Registry | `schema-registry.mekong.local` |
| Flink UI | `flink.mekong.local` |
| Spark History | `spark-history.mekong.local` |
| JupyterLab | `jupyter.mekong.local` |
| Dagster UI | `dagster.mekong.local` |
| Grafana (logs) | `grafana.mekong.local` |

## Secrets

Real secret values are not committed. `k8s/secrets/*.yaml` files are
gitignored. To set up secrets locally:

1. Copy each `*.yaml` template, fill in base64-encoded values.
2. `make k8s-secrets` — applies them and mirrors to all consuming namespaces.

For production, replace this with your secrets manager (Vault, AWS SSM, etc.).

## Design docs

See `design/`:
- `DESIGN.md` — top-level architecture
- `services/` — one doc per major service (Kafka, MinIO, Flink, Spark,
  Dagster, Jupyter, Logging, K8s)
