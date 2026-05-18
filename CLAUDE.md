# Mekong Data Platform — CLAUDE.md

This file is loaded by Claude Code for every repo under this folder.
Repo-specific instructions live in each repo's own `CLAUDE.md`.

## Data Engineer Skill

**Always invoke the data engineer skill at the start of every Claude session:**

```
/mcpmarket-my-toolkit:data-engineer
```

This applies to all work under the Mekong platform — architecture decisions,
pipeline design, schema changes, migration planning, and code review.

## Platform Overview

Mekong is a scalable market-data platform built on an open-source streaming
and batch stack. It ingests real-time stock and crypto prices, stores them in
a data lake, and derives OHLCV bars, technical indicators, and market reports
via scheduled Spark jobs orchestrated by Dagster.

See `MIGRATION.md` in this folder for the full multi-repo split strategy.

## Repository Map

| Repo | Status | Purpose |
|---|---|---|
| `mekong-infra/` | Active | Docker Compose, Kafka topics, MinIO buckets |
| `mekong-data-models/` | Active | Shared schemas, Avro specs, Kafka topic contracts |
| `mekong-kafka/` | Active | Producers + storage consumer (Kafka → MinIO) |
| `mekong-jobs/` | Active | Flink stream jobs + Spark batch jobs |
| `mekong-notebooks/` | Active | Jupyter notebooks for exploration and analysis |
| `mekong-dagster/` | Active | Dagster orchestration — asset graph, schedules, Spark integration |

## Naming Conventions

- Repo names: `mekong-<service>` (kebab-case)
- Kafka topics: `<asset_class>.price.realtime` (dot-separated)
- MinIO prefixes: `<event_type>/asset_class=<val>/year=/month=/day=/` (Hive partitioning)
- Python packages: `mekong_<service>` (snake_case)
- Git branches: `feature/<phase-or-feature-name>`

## Shared Rules (apply to every repo)

- Never commit secrets — use `.env` for local dev, GitHub Secrets for CI
- **Never commit `.env` or any `.env.*` file to git.** Every repo has `.env` and `.env.*` in `.gitignore`. Do not create `.env.example` or any variant and stage it — if default values need documenting, put them in the `README.md` instead.
- Every Kafka topic and MinIO path boundary has an explicit contract documented in `MIGRATION.md`
- Schema changes to `mekong-data-models` follow semver: additive = minor bump, breaking = major bump
- No notebook output committed — enforce with `nbstripout` pre-commit hook in `mekong-notebooks`
- Production logic extracted from a notebook goes into `mekong-jobs`, not back into the notebook repo

## Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka 4.0 (KRaft mode) |
| Object storage | MinIO (S3-compatible) |
| Stream processing | Apache Flink (PyFlink) |
| Batch processing | Apache Spark |
| Orchestration | Dagster 1.13 |
| Notebooks | JupyterLab |
| Raw format | Avro (Kafka → MinIO) |
| Analytical format | Parquet (Spark output) |
