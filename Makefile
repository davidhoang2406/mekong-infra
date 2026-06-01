.PHONY: k8s-namespaces k8s-secrets k8s-rbac k8s-operators \
        k8s-data-up k8s-processing-up k8s-pipeline-up k8s-dagster-up k8s-logging-up k8s-dev-up \
        k8s-platform-up k8s-platform-down k8s-api-image \
        k8s-up k8s-down k8s-status \
        k8s-topics-create k8s-minio-init \
        platform-up platform-down build-api build-ws build-web

COMPOSE := docker compose -f docker-compose.yml

KUBECTL := kubectl
HELM    := helm

# ── Operators (run once) ──────────────────────────────────────────────────────

k8s-operators: ## Install Flink and Spark K8s operators via Helm (run once)
	$(HELM) repo add flink-operator-repo https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.10.0/
	$(HELM) repo add spark-operator https://kubeflow.github.io/spark-operator
	$(HELM) repo update
	$(HELM) upgrade --install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
		--namespace mekong-processing --create-namespace
	$(HELM) upgrade --install spark-operator spark-operator/spark-operator \
		--namespace mekong-processing \
		--set webhook.enable=true \
		--set sparkJobNamespace=mekong-processing

# ── Bootstrap ─────────────────────────────────────────────────────────────────

k8s-namespaces: ## Create all mekong namespaces
	$(KUBECTL) apply -f k8s/mekong-data/namespace.yaml
	$(KUBECTL) apply -f k8s/mekong-processing/namespace.yaml
	$(KUBECTL) apply -f k8s/mekong-pipeline/namespace.yaml
	$(KUBECTL) apply -f k8s/mekong-orchestration/namespace.yaml
	$(KUBECTL) apply -f k8s/mekong-observability/namespace.yaml
	$(KUBECTL) apply -f k8s/mekong-dev/namespace.yaml
	$(KUBECTL) apply -f k8s/mekong-platform/namespace.yaml

k8s-secrets: ## Apply secrets (fill in k8s/secrets/ values first — files are gitignored)
	@for f in k8s/secrets/minio-credentials.yaml k8s/secrets/dagster-postgres.yaml k8s/secrets/telegram-credentials.yaml; do \
		if grep -q '<base64-' $$f 2>/dev/null; then \
			echo "ERROR: $$f still has placeholder values — fill them in before applying."; exit 1; \
		fi; \
		$(KUBECTL) apply -f $$f; \
	done
	@if [ -f k8s/secrets/jupyter-credentials.yaml ] && ! grep -q '<base64-' k8s/secrets/jupyter-credentials.yaml; then \
		$(KUBECTL) apply -f k8s/secrets/jupyter-credentials.yaml; \
	fi
	@echo "Mirroring minio-credentials to all namespaces..."
	@MINIO_ACCESS=$$($(KUBECTL) get secret minio-credentials -n mekong-data -o jsonpath='{.data.access-key}' | base64 -d); \
	MINIO_SECRET=$$($(KUBECTL) get secret minio-credentials -n mekong-data -o jsonpath='{.data.secret-key}' | base64 -d); \
	for ns in mekong-pipeline mekong-processing mekong-orchestration mekong-dev mekong-platform; do \
		$(KUBECTL) create secret generic minio-credentials -n $$ns \
			--from-literal=access-key="$$MINIO_ACCESS" \
			--from-literal=secret-key="$$MINIO_SECRET" \
			--dry-run=client -o yaml | $(KUBECTL) apply -f - || exit 1; \
	done
	@echo "Mirroring telegram-credentials to processing and orchestration..."
	@TG_TOKEN=$$($(KUBECTL) get secret telegram-credentials -n mekong-data -o jsonpath='{.data.bot-token}' | base64 -d); \
	TG_CHAT=$$($(KUBECTL) get secret telegram-credentials -n mekong-data -o jsonpath='{.data.chat-id}' | base64 -d); \
	for ns in mekong-processing mekong-orchestration; do \
		$(KUBECTL) create secret generic telegram-credentials -n $$ns \
			--from-literal=bot-token="$$TG_TOKEN" \
			--from-literal=chat-id="$$TG_CHAT" \
			--dry-run=client -o yaml | $(KUBECTL) apply -f - || exit 1; \
	done

k8s-rbac: ## Apply RBAC roles and bindings
	$(KUBECTL) apply -f k8s/rbac/

# ── Service groups ────────────────────────────────────────────────────────────

k8s-data-up: ## Deploy Kafka + MinIO + Schema Registry + Kafka UI
	$(KUBECTL) apply -f k8s/mekong-data/namespace.yaml
	$(KUBECTL) apply -f k8s/mekong-data/kafka-services.yaml
	$(KUBECTL) apply -f k8s/mekong-data/kafka-statefulset.yaml
	$(KUBECTL) apply -f k8s/mekong-data/minio-services.yaml
	$(KUBECTL) apply -f k8s/mekong-data/minio-statefulset.yaml
	$(KUBECTL) apply -f k8s/mekong-data/schema-registry-deployment.yaml
	$(KUBECTL) apply -f k8s/mekong-data/kafka-ui-deployment.yaml
	$(KUBECTL) apply -f k8s/ingress.yaml

k8s-topics-create: ## Create Kafka topics via K8s Job (run after k8s-data-up)
	$(KUBECTL) apply -f k8s/mekong-data/kafka-topics-job.yaml

k8s-minio-init: ## Create MinIO buckets and lifecycle rules via K8s Job
	$(KUBECTL) apply -f k8s/mekong-data/minio-init-job.yaml

k8s-processing-up: ## Deploy Flink + Spark History (requires k8s-operators first)
	$(KUBECTL) apply -f k8s/mekong-processing/namespace.yaml
	$(KUBECTL) apply -f k8s/mekong-processing/flink-deployment.yaml
	$(KUBECTL) apply -f k8s/mekong-processing/spark-history-deployment.yaml

k8s-pipeline-up: ## Deploy producers + storage consumer
	$(KUBECTL) apply -f k8s/mekong-pipeline/namespace.yaml
	$(KUBECTL) apply -f k8s/mekong-pipeline/stock-price-producer-deployment.yaml
	$(KUBECTL) apply -f k8s/mekong-pipeline/crypto-price-producer-deployment.yaml
	$(KUBECTL) apply -f k8s/mekong-pipeline/storage-consumer-deployment.yaml

k8s-dagster-up: ## Deploy PostgreSQL + Dagster webserver + daemon
	$(KUBECTL) apply -f k8s/mekong-orchestration/namespace.yaml
	$(KUBECTL) apply -f k8s/mekong-orchestration/postgres-statefulset.yaml
	$(KUBECTL) apply -f k8s/mekong-orchestration/dagster-configmap.yaml
	$(KUBECTL) apply -f k8s/mekong-orchestration/dagster-webserver-deployment.yaml
	$(KUBECTL) apply -f k8s/mekong-orchestration/dagster-daemon-deployment.yaml

k8s-logging-up: ## Deploy Loki + Promtail + Grafana via Helm
	$(HELM) repo add grafana https://grafana.github.io/helm-charts
	$(HELM) repo update
	$(HELM) upgrade --install loki grafana/loki-stack \
		-n mekong-observability \
		--create-namespace \
		-f k8s/mekong-observability/values-loki-stack.yaml

k8s-dev-up: ## Deploy JupyterLab
	$(KUBECTL) apply -f k8s/mekong-dev/namespace.yaml
	$(KUBECTL) apply -f k8s/mekong-dev/jupyter-deployment.yaml

# ── Full stack ────────────────────────────────────────────────────────────────

k8s-up: k8s-namespaces k8s-secrets k8s-rbac k8s-data-up k8s-processing-up k8s-pipeline-up k8s-dagster-up k8s-logging-up k8s-dev-up ## Bring up full K8s stack
	@echo ""
	@echo "Stack deployed. Run these next:"
	@echo "  make k8s-topics-create    — create Kafka topics"
	@echo "  make k8s-minio-init       — create MinIO buckets"
	@echo ""
	@echo "If first-time install, ensure operators were installed first:"
	@echo "  make k8s-operators"

k8s-status: ## Show pod status across all mekong namespaces
	@echo "=== mekong-data ==="
	$(KUBECTL) get pods -n mekong-data
	@echo ""
	@echo "=== mekong-processing ==="
	$(KUBECTL) get pods -n mekong-processing
	@echo ""
	@echo "=== mekong-pipeline ==="
	$(KUBECTL) get pods -n mekong-pipeline
	@echo ""
	@echo "=== mekong-orchestration ==="
	$(KUBECTL) get pods -n mekong-orchestration
	@echo ""
	@echo "=== mekong-observability ==="
	$(KUBECTL) get pods -n mekong-observability
	@echo ""
	@echo "=== mekong-dev ==="
	$(KUBECTL) get pods -n mekong-dev

k8s-down: ## Delete all mekong K8s resources (PVCs preserved — data survives)
	$(KUBECTL) delete -f k8s/mekong-pipeline/ --ignore-not-found
	$(KUBECTL) delete -f k8s/mekong-dev/ --ignore-not-found
	$(KUBECTL) delete -f k8s/mekong-orchestration/ --ignore-not-found
	$(KUBECTL) delete -f k8s/mekong-processing/ --ignore-not-found
	$(KUBECTL) delete -f k8s/mekong-data/ --ignore-not-found
	$(KUBECTL) delete -f k8s/rbac/ --ignore-not-found
	$(KUBECTL) delete -f k8s/ingress.yaml --ignore-not-found
	$(HELM) uninstall loki -n mekong-observability --ignore-not-found

# ── Docker Compose — Data Platform ───────────────────────────────────────────

build-api: ## Build mekong-api Docker image
	$(COMPOSE) build mekong-api

build-ws: ## Build mekong-ws Docker image
	$(COMPOSE) build mekong-ws

build-web: ## Build mekong-web Docker image
	$(COMPOSE) build mekong-web

platform-up: ## Start data platform (Postgres + API + Kong) → http://localhost:3002
	$(COMPOSE) up -d postgres mekong-api kong

platform-down: ## Stop data platform
	$(COMPOSE) stop kong mekong-api postgres

# ── Kubernetes — Data Platform ────────────────────────────────────────────────

k8s-api-image: ## Build mekong-api image inside minikube's Docker daemon
	cd ../mekong-api && eval $$(minikube docker-env) && docker build -t mekong-api:latest .

k8s-platform-up: ## Deploy mekong-platform namespace, Postgres, mekong-api → api.mekong.local
	$(KUBECTL) apply -f k8s/mekong-platform/namespace.yaml
	$(KUBECTL) apply -f k8s/secrets/platform-postgres.yaml
	@MINIO_ACCESS=$$($(KUBECTL) get secret minio-credentials -n mekong-data -o jsonpath='{.data.access-key}' | base64 -d); \
	MINIO_SECRET=$$($(KUBECTL) get secret minio-credentials -n mekong-data -o jsonpath='{.data.secret-key}' | base64 -d); \
	$(KUBECTL) create secret generic minio-credentials -n mekong-platform \
		--from-literal=access-key="$$MINIO_ACCESS" \
		--from-literal=secret-key="$$MINIO_SECRET" \
		--dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(KUBECTL) apply -f k8s/mekong-platform/postgres-statefulset.yaml
	$(KUBECTL) apply -f k8s/mekong-platform/mekong-api-deployment.yaml
	$(KUBECTL) apply -f k8s/ingress.yaml

k8s-platform-down: ## Remove mekong-platform resources
	$(KUBECTL) delete -f k8s/mekong-platform/mekong-api-deployment.yaml --ignore-not-found
	$(KUBECTL) delete -f k8s/mekong-platform/postgres-statefulset.yaml --ignore-not-found
	$(KUBECTL) delete -f k8s/mekong-platform/namespace.yaml --ignore-not-found
