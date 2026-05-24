.PHONY: up down prod-up \
        build build-flink build-spark build-dagster build-jupyter prod-build \
        topics-create minio-init storage-flush \
        flink-up spark-up dagster-up dagster-down jupyter-up \
        dagster-logs dagster-shell \
        logging-up logging-down logging-logs \
        install uninstall \
        ci

PYTHON       := .venv/bin/python
PIP          := .venv/bin/pip
COMPOSE      := docker compose
COMPOSE_PROD := docker compose -f docker-compose.yml

# ── Venv setup ────────────────────────────────────────────────────────────────

.venv:
	python3.12 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

# ── Full stack ────────────────────────────────────────────────────────────────

up: ## Start all services — dev mode (override mounts live mekong-jobs)
	$(COMPOSE) up -d

prod-up: ## Start all services — production mode (baked mekong-jobs, no live mount)
	$(COMPOSE_PROD) up -d

down: ## Stop all services (volumes are preserved)
	$(COMPOSE) down

# ── Image builds ──────────────────────────────────────────────────────────────

build-flink: ## Build Flink image (bakes mekong-jobs from GitHub)
	$(COMPOSE) build flink-jobmanager flink-taskmanager

build-spark: ## Build Spark image (bakes mekong-jobs from GitHub)
	$(COMPOSE) build spark-master spark-worker spark-history-server

build-dagster: ## Build Dagster Docker image
	$(COMPOSE) build dagster-webserver dagster-daemon

build-jupyter: ## Build Jupyter Docker image (downloads S3A JARs + installs deps)
	$(COMPOSE) build jupyter

build: build-flink build-spark build-dagster build-jupyter ## Build all images

prod-build: ## Rebuild Flink + Spark images from scratch (fetches latest mekong-jobs)
	$(COMPOSE) build --no-cache flink-jobmanager flink-taskmanager spark-master spark-worker spark-history-server

# ── Per-service start ─────────────────────────────────────────────────────────

flink-up: ## Start Kafka + Flink cluster
	$(COMPOSE) up -d kafka kafka-ui flink-jobmanager flink-taskmanager

spark-up: ## Start MinIO + Spark cluster
	$(COMPOSE) up -d minio spark-master spark-worker spark-history-server

dagster-up: ## Start MinIO + Spark + Dagster (webserver + daemon) → http://localhost:3000
	$(COMPOSE) up -d minio spark-master spark-worker dagster-webserver dagster-daemon

dagster-down: ## Stop Dagster webserver + daemon
	$(COMPOSE) stop dagster-webserver dagster-daemon

dagster-logs: ## Tail Dagster webserver + daemon logs
	$(COMPOSE) logs -f dagster-webserver dagster-daemon

dagster-shell: ## Open a shell in the dagster-webserver container
	docker exec -it dagster-webserver bash

jupyter-up: ## Start MinIO + Jupyter → http://localhost:8888 (set JUPYTER_TOKEN=<token> in .env to require a token)
	$(COMPOSE) up -d minio jupyter

# ── Infrastructure initialisation ─────────────────────────────────────────────

# NOTE: replication-factor 1 — dev only; match to broker count before using in production
topics-create: ## Create Kafka topics (safe to re-run — uses --if-not-exists)
	docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic stock.price.realtime  --partitions 6 --replication-factor 1
	docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic crypto.price.realtime --partitions 6 --replication-factor 1

minio-init: .venv ## Create MinIO buckets and apply lifecycle rules (safe to re-run)
	$(PYTHON) db/init_minio.py

storage-flush: ## Selectively delete objects from MinIO buckets (irreversible)
	@echo "WARNING: this permanently deletes all data from selected buckets."
	@read -p "  Delete market-data (raw price snapshots)? [y/n] " md; \
	read -p "  Delete market-analysis (OHLCV bars)? [y/n] " ma; \
	if [ "$$md" != "y" ] && [ "$$ma" != "y" ]; then \
		echo "Nothing selected — aborted."; \
	else \
		if [ "$$md" = "y" ]; then \
			$(PYTHON) db/flush_minio.py market-data; \
		fi; \
		if [ "$$ma" = "y" ]; then \
			$(PYTHON) db/flush_minio.py market-analysis; \
		fi; \
		echo "Flush complete."; \
	fi

# ── Logging stack ─────────────────────────────────────────────────────────────

logging-up: ## Start Loki + Promtail + Grafana → http://localhost:3001
	$(COMPOSE) up -d loki promtail grafana

logging-down: ## Stop logging stack
	$(COMPOSE) stop loki promtail grafana

logging-logs: ## Tail Loki + Promtail logs
	$(COMPOSE) logs -f loki promtail

# ── Interactive installer ─────────────────────────────────────────────────────

ci: .venv ## Lint Python scripts and validate Docker Compose config (no containers required)
	$(PIP) install --quiet ruff
	.venv/bin/ruff check db/
	$(COMPOSE) config --quiet

install: .venv ## Interactively start selected services and initialise infrastructure
	@echo "Select services to start:"
	@read -p "  Kafka + Kafka UI? [y/n] " k; \
	read -p "  MinIO (object storage)? [y/n] " m; \
	read -p "  Flink (JobManager + TaskManager)? [y/n] " fl; \
	read -p "  Spark (Master + Worker + History Server)? [y/n] " sp; \
	read -p "  Jupyter (JupyterLab at :8888)? [y/n] " jup; \
	read -p "  Dagster (webserver + daemon at :3000)? [y/n] " dag; \
	read -p "  Logging stack (Loki + Promtail + Grafana at :3001)? [y/n] " log; \
	if [ "$$k" != "y" ] && [ "$$m" != "y" ] && [ "$$fl" != "y" ] && [ "$$sp" != "y" ] && [ "$$jup" != "y" ] && [ "$$dag" != "y" ] && [ "$$log" != "y" ]; then \
		echo "Nothing selected — aborted."; \
	else \
		services=""; \
		if [ "$$k" = "y" ]; then services="$$services kafka kafka-ui"; fi; \
		if [ "$$m" = "y" ]; then services="$$services minio"; fi; \
		if [ "$$fl" = "y" ]; then services="$$services flink-jobmanager flink-taskmanager"; fi; \
		if [ "$$sp" = "y" ]; then services="$$services spark-master spark-worker spark-history-server"; fi; \
		if [ "$$jup" = "y" ]; then services="$$services jupyter"; fi; \
		if [ "$$dag" = "y" ]; then services="$$services dagster-webserver dagster-daemon"; fi; \
		if [ "$$log" = "y" ]; then services="$$services loki promtail grafana"; fi; \
		if [ "$$fl" = "y" ]; then \
			echo "Building PyFlink Docker image..."; \
			$(COMPOSE) build flink-jobmanager flink-taskmanager; \
		fi; \
		if [ "$$sp" = "y" ]; then \
			echo "Building Spark Docker image (downloads S3A JARs — takes a moment)..."; \
			$(COMPOSE) build spark-master spark-worker spark-history-server; \
		fi; \
		if [ "$$jup" = "y" ]; then \
			echo "Building Jupyter Docker image (downloads JARs + installs deps — takes a moment)..."; \
			$(COMPOSE) build jupyter; \
		fi; \
		if [ "$$dag" = "y" ]; then \
			echo "Building Dagster Docker image..."; \
			$(COMPOSE) build dagster-webserver dagster-daemon; \
		fi; \
		echo "Starting:$$services"; \
		$(COMPOSE) up -d $$services; \
		if [ "$$m" = "y" ]; then \
			echo "Waiting for MinIO..."; \
			until curl -sf http://localhost:9000/minio/health/live 2>/dev/null; do \
				printf '.'; sleep 2; \
			done; \
			echo ""; \
			$(MAKE) minio-init; \
		fi; \
		if [ "$$k" = "y" ]; then \
			echo "Waiting for Kafka..."; \
			until docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null; do \
				printf '.'; sleep 2; \
			done; \
			echo ""; \
			$(MAKE) topics-create; \
		fi; \
		echo "Setup complete."; \
	fi

uninstall: ## Stop all services and remove named volumes (irreversible — destroys all persisted data)
	@echo "WARNING: this permanently removes all containers and named volumes"
	@echo "         (Kafka data, MinIO objects, Loki logs, Grafana dashboards, etc.)."
	@read -p "  Remove everything? [y/n] " confirm; \
	if [ "$$confirm" = "y" ]; then \
		$(COMPOSE) down -v; \
		echo "All services and volumes removed."; \
	else \
		echo "Aborted."; \
	fi
