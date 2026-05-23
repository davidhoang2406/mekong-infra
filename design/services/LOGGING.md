# Centralised Logging — Grafana Loki + Promtail

## 1. Role

All Mekong services (Kafka, Flink, Spark, Dagster, MinIO, Jupyter) emit logs to
Docker stdout/stderr with no central collection point. Diagnosing failures today
means knowing which container to `docker logs` — and if a container has cycled,
those logs are gone.

This document covers deploying **Loki + Promtail + Grafana** as the centralised
log stack. Promtail tails every container's stdout/stderr automatically; Loki
stores and indexes logs by label; Grafana lets you search, filter, and correlate
logs from all services in one browser tab.

---

## 2. Why Loki over alternatives

| Criterion | Loki | ELK (Elastic) | Splunk |
|---|---|---|---|
| **Resource cost** | ~256 MB RAM total | 2–4 GB (Elasticsearch alone) | Enterprise-licensed |
| **Fits local Docker stack** | Yes — 3 small containers | Barely — heavy on laptop | No — cost-prohibitive |
| **Zero app changes** | Yes — Promtail reads Docker logs automatically | Yes (with Filebeat) | Yes (with UF) |
| **Full-text search** | Label-indexed only; content search via grep | Full inverted index | Full inverted index |
| **Future Grafana integration** | Native — same UI as Prometheus dashboards | Requires Kibana (separate UI) | Separate UI |
| **Kubernetes path** | Loki Helm chart, same config | ECK operator | Splunk Operator |

**Decision:** Loki's lightweight label-indexed model is the right fit for a local
Docker Compose stack. Full-text search across log content is rarely needed — you
want *"show me all Flink logs for the last 30 minutes"* or *"show me Dagster errors
today"*, both of which LogQL handles natively with label filters. If query
complexity grows, the ELK upgrade path is straightforward (replace Loki with
Elasticsearch; keep Promtail/Grafana).

An additional benefit: the same Grafana instance can host Prometheus metrics
dashboards (consumer lag from SUGGESTION.md §1.4) alongside logs — one UI for the
entire observability stack.

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Docker host                                             │
│                                                         │
│  kafka  flink-*  spark-*  dagster-*  minio  jupyter     │
│    │       │        │         │        │       │         │
│    └───────┴────────┴─────────┴────────┴───────┘         │
│                   stdout / stderr                        │
│                        │                                │
│              ┌─────────▼──────────┐                     │
│              │     promtail       │ ← /var/run/docker.sock│
│              │  (Docker SD)       │                     │
│              └─────────┬──────────┘                     │
│                        │  HTTP push                     │
│              ┌─────────▼──────────┐                     │
│              │       loki         │ :3100               │
│              │  (log storage)     │                     │
│              └─────────┬──────────┘                     │
│                        │  LogQL                         │
│              ┌─────────▼──────────┐                     │
│              │      grafana       │ :3001               │
│              │  (UI + dashboards) │                     │
│              └────────────────────┘                     │
└─────────────────────────────────────────────────────────┘
```

Promtail uses Docker service discovery (`docker_sd_configs`) to find every
running container automatically — no per-service configuration is needed when
new containers are added.

Each log line is labelled with:

| Label | Source | Example |
|---|---|---|
| `container` | Docker container name | `flink-jobmanager` |
| `service` | Docker Compose service name | `flink-jobmanager` |
| `stream` | stdout or stderr | `stderr` |
| `project` | Docker Compose project name | `mekong-infra` |

---

## 4. Configuration Files

All configuration lives under `mekong-infra/logging/`.

### 4.1 `logging/loki-config.yaml`

```yaml
auth_enabled: false

server:
  http_listen_port: 3100
  grpc_listen_port: 9096

common:
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory

schema_config:
  configs:
    - from: 2024-01-01
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h

limits_config:
  retention_period: 720h   # 30 days — matches MinIO market-data lifecycle
  ingestion_rate_mb: 16
  ingestion_burst_size_mb: 32

compactor:
  working_directory: /loki/compactor
  compaction_interval: 10m
  retention_enabled: true
  retention_delete_delay: 2h
```

### 4.2 `logging/promtail-config.yaml`

```yaml
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml   # tracks read position per log file

clients:
  - url: http://loki:3100/loki/api/v1/push

scrape_configs:
  - job_name: containers
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
        refresh_interval: 5s
        filters:
          - name: status
            values: [running]
    relabel_configs:
      # strip leading slash from container name: /kafka → kafka
      - source_labels: [__meta_docker_container_name]
        regex: /(.*)
        target_label: container
      # stdout vs stderr
      - source_labels: [__meta_docker_container_log_stream]
        target_label: stream
      # compose service name (e.g. flink-jobmanager)
      - source_labels: [__meta_docker_container_label_com_docker_compose_service]
        target_label: service
      # compose project (e.g. mekong-infra)
      - source_labels: [__meta_docker_container_label_com_docker_compose_project]
        target_label: project
    pipeline_stages:
      - docker: {}   # parses Docker JSON log envelope; promotes .log field as the line
```

### 4.3 `logging/grafana/provisioning/datasources/loki.yaml`

Grafana auto-provisions the Loki datasource on first start — no manual setup needed.

```yaml
apiVersion: 1
datasources:
  - name: Loki
    type: loki
    access: proxy
    url: http://loki:3100
    isDefault: true
    version: 1
    editable: false
    jsonData:
      maxLines: 5000
```

---

## 5. Docker Compose Changes

Add three services to `docker-compose.yml` and one new named volume.

### New services

```yaml
  loki:
    image: grafana/loki:3.4.2
    container_name: loki
    ports:
      - "3100:3100"
    command: -config.file=/etc/loki/loki-config.yaml
    volumes:
      - ./logging/loki-config.yaml:/etc/loki/loki-config.yaml:ro
      - loki_data:/loki
    healthcheck:
      test: ["CMD", "wget", "--quiet", "--tries=1", "--spider", "http://localhost:3100/ready"]
      interval: 15s
      timeout: 5s
      retries: 5
      start_period: 20s

  promtail:
    image: grafana/promtail:3.4.2
    container_name: promtail
    command: -config.file=/etc/promtail/promtail-config.yaml
    volumes:
      - ./logging/promtail-config.yaml:/etc/promtail/promtail-config.yaml:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    depends_on:
      loki:
        condition: service_healthy

  grafana:
    image: grafana/grafana:11.6.1
    container_name: grafana
    ports:
      - "3001:3000"   # 3000 is taken by dagster-webserver
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
      GF_AUTH_DISABLE_LOGIN_FORM: "true"
    volumes:
      - ./logging/grafana/provisioning:/etc/grafana/provisioning:ro
      - grafana_data:/var/lib/grafana
    depends_on:
      loki:
        condition: service_healthy
```

### New volumes (add to the `volumes:` block)

```yaml
volumes:
  # ... existing volumes ...
  loki_data:
  grafana_data:
```

> **Note on `GF_AUTH_ANONYMOUS_ENABLED`:** Grafana is set to allow anonymous
> admin access — appropriate for a local dev stack. Remove these env vars and
> set a password if this is ever exposed on a shared or remote machine.

---

## 6. How to Deploy

### Step 1 — Create the config files

```bash
cd mekong-infra
mkdir -p logging/grafana/provisioning/datasources

# create loki-config.yaml, promtail-config.yaml, and loki.yaml
# with the content from §4 above
```

### Step 2 — Apply the docker-compose changes

Add the three services and two volumes from §5 to `docker-compose.yml`.

### Step 3 — Start the logging stack

```bash
# Start only the logging services (doesn't touch existing containers)
docker compose up -d loki promtail grafana

# Or add to the Makefile logging-up target (see §7)
make logging-up
```

### Step 4 — Verify

```bash
# Loki ready?
curl http://localhost:3100/ready
# → ready

# Promtail targets discovered?
curl http://localhost:9080/targets
# → JSON list of all running containers

# Open Grafana
open http://localhost:3001
# → Go to Explore → select Loki → run: {project="mekong-infra"}
```

---

## 7. Makefile Targets to Add

```makefile
logging-up: ## Start Loki + Promtail + Grafana → http://localhost:3001
	$(COMPOSE) up -d loki promtail grafana

logging-down: ## Stop logging stack
	$(COMPOSE) stop loki promtail grafana

logging-logs: ## Tail Loki + Promtail logs
	$(COMPOSE) logs -f loki promtail
```

---

## 8. Useful LogQL Queries

Once Grafana is running, use these queries in **Explore → Loki**:

| Goal | Query |
|---|---|
| All logs from all Mekong containers | `{project="mekong-infra"}` |
| Flink logs only | `{service=~"flink-.*"}` |
| Dagster errors only | `{service=~"dagster-.*"} \|= "ERROR"` |
| Spark job failures | `{service=~"spark-.*"} \|= "ERROR" \|= "Exception"` |
| Kafka broker warnings | `{container="kafka"} \|= "WARN"` |
| Flink logs for a specific symbol | `{service=~"flink-.*"} \|= "VCB"` |
| All stderr across all services | `{project="mekong-infra", stream="stderr"}` |
| Dagster sensor evaluations | `{service=~"dagster-.*"} \|= "SensorDaemon"` |
| Price alert Telegram sends | `{service=~"flink-.*"} \|= "VOLATILITY BURST"` |

LogQL supports full regex and line filters. See the
[Loki LogQL docs](https://grafana.com/docs/loki/latest/query/) for aggregations
(e.g. error rate per service over time).

---

## 9. Containers and Ports

| Service | Image | Port | Purpose |
|---|---|---|---|
| `loki` | `grafana/loki:3.4.2` | `3100` | Log ingestion and storage |
| `promtail` | `grafana/promtail:3.4.2` | `9080` (internal) | Log collection from Docker |
| `grafana` | `grafana/grafana:11.6.1` | `3001` | Query UI and dashboards |

Loki data is persisted in the `loki_data` named volume. Grafana dashboard
settings are persisted in `grafana_data`.

---

## 10. Retention

Loki is configured with `retention_period: 720h` (30 days), matching the MinIO
`market-data` bucket lifecycle policy. The compactor runs every 10 minutes and
deletes chunks older than 30 days with a 2-hour delay.

To change retention, edit `retention_period` in `loki-config.yaml` and restart:

```bash
docker compose restart loki
```

---

## 11. Failure Modes and Gotchas

- **Promtail needs the Docker socket.** The `/var/run/docker.sock:/var/run/docker.sock:ro`
  mount is required for Docker SD. If the socket path differs on your OS
  (e.g. Docker Desktop on Mac uses a different socket path), update the mount:
  `~/.docker/run/docker.sock:/var/run/docker.sock:ro`.

- **Grafana port 3001, not 3000.** Dagster webserver already occupies 3000.
  Grafana maps its internal 3000 to host 3001.

- **Log lines before Promtail started are not collected.** Promtail only collects
  logs from containers that are running when it starts, and reads from the current
  tail position. Historical logs from before the logging stack was deployed are
  not backfilled.

- **Loki label cardinality.** Avoid adding high-cardinality labels (e.g. `symbol`,
  `price`) via Promtail pipeline stages. Labels are for routing and filtering, not
  for data values — that belongs in the log line content.

- **`loki_data` volume and schema changes.** Loki schema version is locked at
  first write. If you need to change `schema_config`, delete the `loki_data`
  volume and restart (`docker compose down -v loki && docker compose up -d loki`).

---

## 12. Future Extensions

- **Prometheus + Grafana** — add a `prometheus` service and Kafka JMX exporter to
  cover SUGGESTION.md §1.4 (consumer lag). The same Grafana instance hosts both
  metrics and logs dashboards.
- **Alert rules in Grafana** — define Loki-based alert rules (e.g. error rate
  exceeds threshold) and route to the Telegram webhook, complementing the Dagster
  failure sensor.
- **Kubernetes migration** — Loki has an official Helm chart (`grafana/loki-stack`)
  and Promtail is a standard DaemonSet; the config files in `logging/` are reused
  with minimal changes.
