# Design Document — Mekong Market Data Platform

A web-based market data platform exposing the Mekong pipeline's historical and real-time
data to external consumers via a Go REST API, a Kafka-backed WebSocket feed, and a React
dashboard. The platform serves the same data that the internal pipeline produces — no new
data sources, just a serving and visualisation layer on top of MinIO and Kafka.

---

## 1. Problem Statement

All data in the Mekong platform is locked inside MinIO Parquet files and Kafka topics.
Accessing it requires either a Spark session, a direct MinIO SDK call, or a Kafka consumer.
There is no way for a browser, a mobile app, a third-party integration, or even a simple
`curl` command to query the data.

This document designs a data platform comprising:

1. **`mekong-api`** — a Go REST API serving historical OHLCV, indicators, screener results,
   and digest data from MinIO Parquet via DuckDB.
2. **`mekong-ws`** — a Go WebSocket server that fans out live Kafka price ticks to browser
   clients, with per-symbol subscription.
3. **`mekong-web`** — a React SPA dashboard consuming both the REST API and WebSocket feed,
   rendering charts, tables, and a real-time ticker.

---

## 2. Architecture Overview

**Editable source:** [`design/images/data-platform-architecture.excalidraw`](../images/data-platform-architecture.excalidraw) — open in [excalidraw.com](https://excalidraw.com) or via the VS Code Excalidraw extension.

![data-platform.png](../images/data-platform.png)

### 2.1 Why this split?

| Concern | Service | Reason |
|---|---|---|
| Historical queries | `mekong-api` | Request-response; cacheable; reads Parquet via DuckDB |
| Live streaming | `mekong-ws` | Long-lived connections; Kafka consumer on the server side; push-based |
| UI | `mekong-web` | React SPA; static files served via Kong; calls both services |
| Cross-cutting (routing, CORS, auth, rate-limiting) | Kong API Gateway | Centralised policy enforcement; services stay focused on business logic |

Keeping the REST API and WebSocket server as separate processes allows independent scaling.
The REST API is CPU-bound (DuckDB queries); the WebSocket server is connection-bound
(many idle connections, small payloads). Different resource profiles, different scaling axes.

### 2.2 Why Go + Gin?

- **Gin** provides a mature, battle-tested HTTP framework with built-in request binding,
  validation, middleware chaining, and structured error handling — without the boilerplate
  of raw `net/http`.
- `goroutine`-per-connection model is natural for WebSocket fan-out.
- DuckDB has a stable Go binding (`github.com/marcboeker/go-duckdb`).
- Single static binary — the Docker image is `FROM scratch` + one binary + DuckDB lib.
- Strong concurrency primitives for the hub/channel pattern in the WebSocket server.
- Gin's `gin.Context` provides a clean handler signature (`func(c *gin.Context)`) with
  built-in JSON serialisation, query param binding, and error abort — less glue code than
  raw `net/http` handlers.

### 2.3 Why DuckDB + Postgres (dual-store)?

The API serves two fundamentally different workloads:

**Analytical queries (DuckDB):** OHLCV bars, indicators, digest, screener — all read-only
against Parquet files already in MinIO. DuckDB reads them directly with partition pruning,
zero ETL, zero data duplication. Sub-50ms latency for single-symbol range queries.

**Application state (Postgres):** user accounts, API keys, saved watchlists, rate limit
counters, audit logs — relational, write-heavy, requires ACID guarantees. These don't
exist in MinIO and shouldn't be shoehorned into Parquet.

| Store | Used for | Access pattern |
|---|---|---|
| DuckDB (embedded) | OHLCV, indicators, digest, screener | Read-only against MinIO Parquet |
| Postgres (service) | Users, API keys, watchlists, sessions | Read-write, relational, ACID |

**Why not Postgres for everything?** Postgres *can* read Parquet via `pg_parquet` or
external tables, but it requires loading data into Postgres tables first — a sync pipeline
that duplicates storage and introduces a staleness window. DuckDB reads MinIO Parquet
natively with no intermediate step; the data is always as fresh as the last Spark job.

**Why not DuckDB for everything?** DuckDB is an embedded analytical engine, not a
transactional database. It has no concurrent write support, no row-level locking, no user
management. Application state needs a proper RDBMS.

### 2.4 Why Kong API Gateway?

The platform needs routing, CORS, rate limiting, authentication, and WebSocket upgrade
handling. These are cross-cutting concerns that should live in one place, not scattered
across Go middleware in each service.

| Concern | Without gateway | With Kong |
|---|---|---|
| CORS | Custom middleware in `mekong-api` | One Kong plugin, configured once |
| Rate limiting | Custom token bucket in Go | Kong `rate-limiting` plugin, per-consumer |
| Auth (API keys, JWT) | Custom middleware per service | Kong `key-auth` / `jwt` plugin |
| WebSocket upgrade | nginx config | Kong native WebSocket proxying |
| Request logging | Per-service slog middleware | Kong `file-log` or `tcp-log` plugin |
| Static file serving | Separate nginx container | Kong `serve-static` or upstream to SPA container |
| SSL termination | Manual nginx cert config | Kong or cloud LB |

Kong runs in **DB-less mode** (declarative YAML config) for simplicity — no database
dependency for the gateway itself. Configuration lives in `kong/kong.yml` checked into
`mekong-infra`. If the platform later needs dynamic configuration (admin API, consumer
management), switching to database-backed mode is a one-line change.

**Alternative considered: Traefik.** Traefik is lighter and Docker-native (auto-discovers
services via labels). It is a good choice if the primary need is just routing and TLS.
Kong wins here because the Phase 4 requirements (API key management, per-consumer rate
limiting, JWT auth) map directly to Kong plugins, while Traefik would require ForwardAuth
to an external service for the same features.

### 2.5 Why DuckDB over Spark for serving?

Spark is designed for large-scale distributed batch processing. The API server needs
sub-second latency for single-symbol range queries over a relatively small dataset
(thousands of symbols x hundreds of days = tens of millions of rows). DuckDB:

- Reads Parquet directly from S3-compatible endpoints (MinIO) with partition pruning.
- Sub-50ms query latency for filtered single-symbol range queries.
- In-process — no cluster, no JVM, no startup overhead.
- Zero operational burden (embedded library, not a service).

---

## 3. New Repositories

| Repo | Language | Purpose |
|---|---|---|
| `mekong-api` | Go | REST API server — historical data from MinIO via DuckDB |
| `mekong-ws` | Go | WebSocket server — live price ticks from Kafka |
| `mekong-web` | TypeScript / React | Dashboard SPA |

All three are dockerised and added to `mekong-infra/docker-compose.yml`.

---

## 4. `mekong-api` — REST API Server (Go)

### 4.1 Tech stack

| Component | Library / Tool |
|---|---|
| HTTP framework | `github.com/gin-gonic/gin` |
| DuckDB binding | `github.com/marcboeker/go-duckdb` |
| Postgres driver | `github.com/jackc/pgx/v5` (connection pool via `pgxpool`) |
| Migrations | `github.com/golang-migrate/migrate/v4` |
| S3 / MinIO | DuckDB `httpfs` extension with S3 credentials |
| JSON serialisation | Gin's built-in `c.JSON()` (uses `encoding/json`) |
| Config | Environment variables (12-factor) |
| Logging | `log/slog` (stdlib structured logging) |
| Auth | Handled by Kong `key-auth` / `jwt` plugin (not in Go code) |
| Rate limiting | Handled by Kong `rate-limiting` plugin (not in Go code) |
| CORS | Handled by Kong `cors` plugin (not in Go code) |

### 4.2 DuckDB + MinIO integration

DuckDB reads Parquet directly from MinIO via its `httpfs` extension:

```sql
-- Initialisation (run once on startup)
INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint = 'minio:9000';
SET s3_access_key_id = 'minioadmin';
SET s3_secret_access_key = 'minioadmin';
SET s3_use_ssl = false;
SET s3_url_style = 'path';

-- Query with partition pruning (sub-50ms for single symbol)
SELECT time, symbol, open, high, low, close, volume
FROM read_parquet('s3://market-analysis/ohlcv.bar/**/*.parquet', hive_partitioning=true)
WHERE symbol = 'VCB'
  AND year = '2026' AND month = '05'
ORDER BY time;
```

DuckDB's Parquet reader performs predicate pushdown — it only reads the Hive partitions
that match the filter, and only the columns referenced in the query. This makes it fast
even when the full dataset is large.

### 4.3 Connection management

**DuckDB (analytics):** embedded database — no server, no pool. The Go process opens one
DuckDB connection at startup and shares it across all Gin handlers. DuckDB handles
concurrent readers via MVCC.

```go
// Simplified startup
duckDB, _ := sql.Open("duckdb", "")
duckDB.Exec("INSTALL httpfs; LOAD httpfs; SET s3_endpoint = ...;")
```

**Postgres (application state):** uses `pgxpool` for connection pooling. The pool is sized
to match Gin's concurrency (default: 10 connections, tunable via `POSTGRES_MAX_CONNS`).

```go
pool, _ := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
// pool is shared across all Gin handlers via dependency injection
```

Both stores are injected into Gin handlers via a shared `App` struct:

```go
type App struct {
    DuckDB *sql.DB
    PG     *pgxpool.Pool
}

func (a *App) GetOHLCV(c *gin.Context) {
    symbol := c.Query("symbol")
    // ... query DuckDB
    c.JSON(http.StatusOK, gin.H{"bars": bars})
}
```

### 4.4 API endpoints

All endpoints return JSON. Errors return `{"error": "message"}` with the appropriate HTTP
status code. All timestamps are ISO-8601 UTC.

---

#### `GET /api/v1/symbols`

List all available symbols with metadata.

**Query parameters:**

| Param | Type | Default | Description |
|---|---|---|---|
| `asset_class` | string | — | Filter by `stock` or `crypto` |

**Response:**

```json
{
  "symbols": [
    {
      "symbol": "VCB",
      "asset_class": "stock",
      "exchange": "HOSE",
      "first_date": "2026-05-01",
      "last_date": "2026-05-25"
    },
    {
      "symbol": "BTC-USDT",
      "asset_class": "crypto",
      "exchange": "BINANCE",
      "first_date": "2026-05-01",
      "last_date": "2026-05-25"
    }
  ]
}
```

**DuckDB query:**

```sql
SELECT symbol, asset_class, exchange,
       MIN(time)::DATE AS first_date,
       MAX(time)::DATE AS last_date
FROM read_parquet('s3://market-analysis/ohlcv.bar/**/*.parquet', hive_partitioning=true)
GROUP BY symbol, asset_class, exchange
ORDER BY symbol;
```

**Caching:** this query scans all partitions. Cache the result for 1 hour in-memory (a
simple `sync.RWMutex`-guarded map with a TTL). Invalidate on manual request or timer.

---

#### `GET /api/v1/ohlcv`

Historical OHLCV bars for a symbol.

**Query parameters:**

| Param | Type | Default | Description |
|---|---|---|---|
| `symbol` | string | **required** | e.g. `VCB`, `BTC-USDT` |
| `from` | date | 30 days ago | Start date (inclusive), YYYY-MM-DD |
| `to` | date | today | End date (inclusive), YYYY-MM-DD |
| `asset_class` | string | — | Optional filter (`stock` or `crypto`) |

**Response:**

```json
{
  "symbol": "VCB",
  "asset_class": "stock",
  "exchange": "HOSE",
  "bars": [
    {
      "time": "2026-05-20T00:00:00Z",
      "open": 85000.0,
      "high": 86200.0,
      "low": 84500.0,
      "close": 85800.0,
      "volume": 2345678
    }
  ]
}
```

**DuckDB query:**

```sql
SELECT time, symbol, exchange, asset_class, open, high, low, close, volume
FROM read_parquet('s3://market-analysis/ohlcv.bar/**/*.parquet', hive_partitioning=true)
WHERE symbol = $1
  AND year || '-' || month || '-' || day BETWEEN $2 AND $3
ORDER BY time;
```

**Caching:** per-symbol, per-date-range. Historical bars are immutable (they are only
written once by the daily Spark job), so completed days can be cached indefinitely.
Only the current day (which may be re-computed) should be cache-busted on each request.

---

#### `GET /api/v1/indicators`

Technical indicators for a symbol.

**Query parameters:**

| Param | Type | Default | Description |
|---|---|---|---|
| `symbol` | string | **required** | e.g. `VCB` |
| `from` | date | 30 days ago | Start date |
| `to` | date | today | End date |

**Response:**

```json
{
  "symbol": "VCB",
  "indicators": [
    {
      "time": "2026-05-20T00:00:00Z",
      "close": 85800.0,
      "sma20": 85100.0,
      "sma50": 84200.0,
      "sma200": 82500.0,
      "rsi14": 58.3,
      "macd": 120.5,
      "macd_signal": 95.2,
      "macd_hist": 25.3,
      "bb_upper": 87200.0,
      "bb_mid": 85100.0,
      "bb_lower": 83000.0
    }
  ]
}
```

**DuckDB query:**

```sql
SELECT time, symbol, close,
       sma20, sma50, sma200, rsi14,
       macd, macd_signal, macd_hist,
       bb_upper, bb_mid, bb_lower
FROM read_parquet('s3://market-analysis/technical.indicators/**/*.parquet', hive_partitioning=true)
WHERE symbol = $1
  AND year || '-' || month || '-' || day BETWEEN $2 AND $3
ORDER BY time;
```

---

#### `GET /api/v1/digest`

Daily market digest — top gainers, losers, and volume leaders.

**Query parameters:**

| Param | Type | Default | Description |
|---|---|---|---|
| `date` | date | today | Target date, YYYY-MM-DD |
| `category` | string | — | Filter: `gainer`, `loser`, `volume`, or omit for all |
| `limit` | int | 10 | Max items per category |

**Response:**

```json
{
  "date": "2026-05-20",
  "digest": [
    {
      "category": "gainer",
      "rank": 1,
      "symbol": "FPT",
      "exchange": "HOSE",
      "asset_class": "stock",
      "open": 120000.0,
      "close": 128000.0,
      "volume": 5678901,
      "pct_change": 6.67
    }
  ]
}
```

**DuckDB query:**

```sql
SELECT category, rank, symbol, exchange, asset_class,
       open, close, volume, pct_change
FROM read_parquet('s3://market-analysis/digest/**/*.parquet', hive_partitioning=true)
WHERE year = $1 AND month = $2 AND day = $3
  AND ($4 = '' OR category = $4)
  AND rank <= $5
ORDER BY category, rank;
```

---

#### `GET /api/v1/screener`

Weekly fundamental screener results.

**Query parameters:**

| Param | Type | Default | Description |
|---|---|---|---|
| `year` | string | current year | e.g. `2026` |
| `week` | string | current week | ISO week number, e.g. `21` |

**Response:**

```json
{
  "year": "2026",
  "week": "21",
  "results": [
    {
      "symbol": "VCB",
      "pe_ratio": 14.2,
      "pb_ratio": 2.1,
      "roe": 22.5,
      "eps": 6200.0,
      "de_ratio": 0.8,
      "current_ratio": 1.4
    }
  ]
}
```

**DuckDB query:**

```sql
SELECT symbol, pe_ratio, pb_ratio, roe, eps, de_ratio, current_ratio
FROM read_parquet('s3://market-analysis/screener/**/*.parquet', hive_partitioning=true)
WHERE year = $1 AND week = $2
ORDER BY pe_ratio;
```

---

#### `GET /api/v1/snapshot`

Latest known price for a symbol (most recent tick from MinIO raw data or a cached Kafka
snapshot).

**Query parameters:**

| Param | Type | Default | Description |
|---|---|---|---|
| `symbol` | string | **required** | e.g. `VCB`, `BTC-USDT` |

**Response:**

```json
{
  "symbol": "BTC-USDT",
  "exchange": "BINANCE",
  "price": 68500.0,
  "change": 1200.0,
  "pct_change": 1.78,
  "volume": 1234567890,
  "bid": 68490.0,
  "ask": 68510.0,
  "timestamp": "2026-05-25T14:30:00Z"
}
```

**Implementation:** this endpoint does NOT query MinIO on every request. Instead, the
WebSocket server (`mekong-ws`) maintains an in-memory `lastPrice` map updated on every
Kafka tick. The API server queries this map via an internal gRPC call or a shared Redis
instance (see  5.5 — shared snapshot cache).

---

#### `GET /api/v1/health`

Healthcheck endpoint for Docker Compose and load balancers.

**Response:**

```json
{
  "status": "ok",
  "duckdb": "connected",
  "postgres": "connected",
  "minio": "reachable",
  "uptime_seconds": 3600
}
```

---

### 4.5 Error handling

Gin's `c.AbortWithStatusJSON()` provides consistent error responses. All errors follow
a standard envelope:

```json
{
  "error": "symbol is required",
  "code": "MISSING_PARAM",
  "status": 400
}
```

```go
func (a *App) GetOHLCV(c *gin.Context) {
    symbol := c.Query("symbol")
    if symbol == "" {
        c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
            "error": "symbol is required", "code": "MISSING_PARAM", "status": 400,
        })
        return
    }
    // ...
}
```

| HTTP status | When |
|---|---|
| 400 | Missing or invalid query parameters |
| 404 | No data found for the given symbol/date range |
| 429 | Rate limit exceeded (returned by Kong, not the Go service) |
| 500 | DuckDB query failure, Postgres or MinIO unreachable |

### 4.6 Project structure

```
mekong-api/
  cmd/
    server/
      main.go              # entry point: init stores, register routes, start Gin
  internal/
    config/
      config.go            # env var parsing + validation
    app/
      app.go               # App struct (DuckDB + PG pool), shared by all handlers
      router.go            # Gin router setup, route group registration
    handler/
      ohlcv.go             # GET /api/v1/ohlcv
      indicators.go        # GET /api/v1/indicators
      digest.go            # GET /api/v1/digest
      screener.go          # GET /api/v1/screener
      symbols.go           # GET /api/v1/symbols
      snapshot.go          # GET /api/v1/snapshot
      health.go            # GET /api/v1/health
      user.go              # POST /api/v1/users, GET /api/v1/users/me (Phase 4)
      watchlist.go         # CRUD /api/v1/watchlists (Phase 4)
    store/
      duckdb.go            # DuckDB connection, S3 init, parameterised query builders
      postgres.go          # pgxpool init, migration runner
      cache.go             # in-memory TTL cache (sync.RWMutex + map)
    middleware/
      logging.go           # request/response logging via slog (Gin middleware)
      recovery.go          # panic recovery (wraps gin.Recovery with slog)
    model/
      ohlcv.go             # OHLCVBar struct + JSON tags
      indicator.go         # IndicatorRow struct
      digest.go            # DigestEntry struct
      screener.go          # ScreenerResult struct
      symbol.go            # SymbolInfo struct
      snapshot.go          # PriceSnapshot struct
      user.go              # User, APIKey structs (Phase 4)
      watchlist.go         # Watchlist struct (Phase 4)
  migrations/
    000001_create_users.up.sql
    000001_create_users.down.sql
    000002_create_api_keys.up.sql
    000002_create_api_keys.down.sql
    000003_create_watchlists.up.sql
    000003_create_watchlists.down.sql
  go.mod
  go.sum
  Dockerfile
```

**Gin router setup (`internal/app/router.go`):**

```go
func (a *App) SetupRouter() *gin.Engine {
    r := gin.New()
    r.Use(gin.Recovery(), middleware.SlogLogger())

    v1 := r.Group("/api/v1")
    {
        v1.GET("/health", a.Health)
        v1.GET("/symbols", a.GetSymbols)
        v1.GET("/ohlcv", a.GetOHLCV)
        v1.GET("/indicators", a.GetIndicators)
        v1.GET("/digest", a.GetDigest)
        v1.GET("/screener", a.GetScreener)
        v1.GET("/snapshot", a.GetSnapshot)
    }

    // Phase 4: user-facing endpoints (auth enforced by Kong, not Gin)
    // users := v1.Group("/users")
    // users.POST("", a.CreateUser)
    // users.GET("/me", a.GetCurrentUser)

    return r
}
```

Note: CORS, rate limiting, and auth are handled by Kong — not by Gin middleware. The
Go service trusts that Kong has already validated the request before it arrives.

### 4.7 Dockerfile

```dockerfile
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /mekong-api ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /mekong-api /usr/local/bin/mekong-api
USER nobody
EXPOSE 8090
HEALTHCHECK --interval=15s --timeout=5s CMD wget -qO- http://localhost:8090/api/v1/health || exit 1
CMD ["mekong-api"]
```

Note: `CGO_ENABLED=1` is required because `go-duckdb` uses CGo bindings.

### 4.8 Postgres schema (application state)

Postgres stores all mutable application state. The schema is managed by
`golang-migrate` — migrations live in `mekong-api/migrations/` and run automatically
on startup.

```sql
-- 000001_create_users.up.sql
CREATE TABLE users (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email       TEXT UNIQUE NOT NULL,
    name        TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- 000002_create_api_keys.up.sql
CREATE TABLE api_keys (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID REFERENCES users(id) ON DELETE CASCADE,
    key_hash    TEXT UNIQUE NOT NULL,
    label       TEXT NOT NULL DEFAULT 'default',
    rate_limit  INT NOT NULL DEFAULT 100,          -- requests per minute
    is_active   BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ DEFAULT now(),
    last_used   TIMESTAMPTZ
);

CREATE INDEX idx_api_keys_hash ON api_keys(key_hash) WHERE is_active;

-- 000003_create_watchlists.up.sql
CREATE TABLE watchlists (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID REFERENCES users(id) ON DELETE CASCADE,
    name        TEXT NOT NULL DEFAULT 'Default',
    symbols     TEXT[] NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (user_id, name)
);
```

Postgres also serves as the Dagster backend when the platform upgrades from SQLite
(see SUGGESTION.md §2.2) — one Postgres instance, two databases.

### 4.9 Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8090` | HTTP listen port |
| `GIN_MODE` | `release` | Gin mode: `debug`, `release`, `test` |
| `MINIO_ENDPOINT` | `minio:9000` | S3 endpoint for DuckDB httpfs |
| `MINIO_ACCESS_KEY` | `minioadmin` | |
| `MINIO_SECRET_KEY` | `minioadmin` | |
| `MINIO_ANALYSIS_BUCKET` | `market-analysis` | Bucket name for Parquet data |
| `POSTGRES_URL` | `postgres://mekong:mekong@postgres:5432/mekong_api` | Postgres connection string |
| `POSTGRES_MAX_CONNS` | `10` | pgxpool max connections |
| `CACHE_TTL_SECONDS` | `300` | Default in-memory cache TTL |
| `SNAPSHOT_CACHE_URL` | — | Redis URL for shared snapshot cache (Phase 2) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

---

## 5. `mekong-ws` — WebSocket Server (Go)

### 5.1 Tech stack

| Component | Library / Tool |
|---|---|
| WebSocket | `github.com/gorilla/websocket` |
| Kafka consumer | `github.com/segmentio/kafka-go` |
| JSON | `encoding/json` (stdlib) |
| Config | Environment variables |
| Logging | `log/slog` |

### 5.2 Architecture — Hub and Spoke

The WebSocket server uses the classic Go hub pattern:

```
                                    ┌──────────────────────┐
 Kafka consumer goroutine ─────────►│        Hub           │
 (reads from both price topics)     │                      │
                                    │  subscriptions map:  │
                                    │  "VCB"    → [c1, c3] │
                                    │  "BTC-USDT"→ [c2]    │
                                    │  "*"      → [c4]     │
                                    │                      │
                                    │  broadcast:          │
                                    │  for each tick,      │
                                    │  fan out to matching  │
                                    │  subscribers          │
                                    └───┬──────┬──────┬────┘
                                        │      │      │
                                        ▼      ▼      ▼
                                       c1     c2     c3    (client goroutines)
                                       │      │      │
                                       ▼      ▼      ▼
                                    browser  browser  browser
```

**Components:**

1. **Kafka Consumer Goroutine**: reads from `stock.price.realtime` and `crypto.price.realtime`
   using a single consumer group (`ws-fanout`). On each message, deserialises the JSON
   envelope and sends a `Tick` struct to the Hub's inbound channel.

2. **Hub**: maintains a map of `symbol → set[*Client]`. When a `Tick` arrives, iterates
   over the subscribers for that symbol (plus `*` wildcard subscribers) and sends to each
   client's outbound channel. The Hub also handles subscribe/unsubscribe messages from
   clients, and removes clients on disconnect.

3. **Client Goroutines**: one read goroutine (processes subscribe/unsubscribe messages from
   the browser) and one write goroutine (drains the outbound channel and writes JSON frames
   to the WebSocket) per connection.

### 5.3 Client protocol

**Connection:**

```
ws://localhost:8091/ws
```

**Subscribe to symbols (client → server):**

```json
{"action": "subscribe", "symbols": ["VCB", "BTC-USDT"]}
```

**Unsubscribe (client → server):**

```json
{"action": "unsubscribe", "symbols": ["VCB"]}
```

**Subscribe to all symbols (client → server):**

```json
{"action": "subscribe", "symbols": ["*"]}
```

**Price tick (server → client):**

```json
{
  "type": "tick",
  "symbol": "BTC-USDT",
  "exchange": "BINANCE",
  "asset_class": "crypto",
  "price": 68500.0,
  "change": 1200.0,
  "pct_change": 1.78,
  "volume": 1234567890,
  "bid": 68490.0,
  "ask": 68510.0,
  "timestamp": "2026-05-25T14:30:00Z"
}
```

**Error (server → client):**

```json
{"type": "error", "message": "invalid action"}
```

### 5.4 Backpressure handling

If a client's outbound channel is full (slow consumer), the write goroutine drops the
oldest tick and increments a `dropped_ticks` counter on the client. After N consecutive
drops (configurable, default 100), the server closes the connection with a close frame
containing a `1008 Policy Violation` code and message `"too slow"`.

This prevents a single slow client from blocking the Hub's broadcast loop.

### 5.5 Shared snapshot cache

The WebSocket server maintains an in-memory `lastPrice` map — every time a tick arrives
from Kafka, it updates `lastPrice[symbol]` with the latest values. This map serves two
purposes:

1. **Initial snapshot on subscribe**: when a client subscribes to a symbol, the server
   immediately sends the last known price (if available) so the UI doesn't show a blank
   state until the next live tick arrives.

2. **`GET /api/v1/snapshot` in `mekong-api`**: rather than querying MinIO for the most
   recent Avro file (slow, ~200ms), the REST API reads the latest price from this cache.

**Phase 1 (simple):** `mekong-api` and `mekong-ws` run as separate processes. The snapshot
endpoint in `mekong-api` makes an internal HTTP call to `mekong-ws:8091/internal/snapshot?symbol=X`.
The `/internal/` path is blocked by Kong's `ip-restriction` plugin (Docker network only).

**Phase 2 (Redis):** if the two services need to scale independently (multiple API
replicas), replace the internal HTTP call with a shared Redis instance. The WebSocket
server writes `SET last_price:{symbol} <json> EX 60` on every tick; the API server reads it.
Redis is optional — only add when scaling requires it.

### 5.6 Project structure

```
mekong-ws/
  cmd/
    server/
      main.go              # entry point: Kafka consumer + WebSocket server
  internal/
    config/
      config.go            # env var parsing
    hub/
      hub.go               # subscription registry, broadcast loop
      client.go            # per-connection read/write goroutines
    kafka/
      consumer.go          # Kafka consumer, message deserialisation
    snapshot/
      cache.go             # lastPrice map, internal HTTP handler
    model/
      tick.go              # Tick struct (from Kafka message envelope)
      protocol.go          # ClientMessage (subscribe/unsubscribe), ServerMessage
  go.mod
  go.sum
  Dockerfile
```

### 5.7 Dockerfile

```dockerfile
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /mekong-ws ./cmd/server

FROM scratch
COPY --from=builder /mekong-ws /mekong-ws
USER 65534
EXPOSE 8091
CMD ["/mekong-ws"]
```

No CGo needed — `kafka-go` and `gorilla/websocket` are pure Go.

### 5.8 Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8091` | WebSocket listen port |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address |
| `KAFKA_CONSUMER_GROUP` | `ws-fanout` | Consumer group ID |
| `MAX_CLIENTS` | `1000` | Max simultaneous WebSocket connections |
| `CLIENT_CHANNEL_SIZE` | `256` | Per-client outbound channel buffer |
| `DROP_THRESHOLD` | `100` | Consecutive drops before force-disconnect |
| `LOG_LEVEL` | `info` | |

---

## 6. `mekong-web` — React Dashboard

### 6.1 Tech stack

| Component | Library |
|---|---|
| Framework | React 19 + TypeScript |
| Build tool | Vite |
| Routing | React Router v7 |
| State management | TanStack Query (REST) + Zustand (WebSocket state) |
| Charts | Lightweight Charts (TradingView) for candlestick, Recharts for indicators |
| UI framework | Tailwind CSS + shadcn/ui |
| Data tables | TanStack Table |
| WebSocket | Native `WebSocket` API + Zustand store |
| HTTP client | `fetch` (native) + TanStack Query |

### 6.2 Page structure

```
/                           → Dashboard (overview)
/symbol/:symbol             → Symbol detail (OHLCV chart + indicators + live ticker)
/screener                   → Weekly screener table
/digest                     → Daily digest (gainers / losers / volume)
/settings                   → User preferences (theme, default date range, etc.)
```

### 6.3 Dashboard page (`/`)

The landing page provides a market overview at a glance.

**Layout:**

```
┌─────────────────────────────────────────────────────────────────┐
│  Live Ticker Bar (horizontal scroll)                            │
│  VCB 85,800 +0.59%  |  FPT 128,000 +2.1%  |  BTC 68,500 +1.7% │
├────────────────────────────────┬────────────────────────────────┤
│  Top Gainers (table)           │  Top Losers (table)            │
│  FPT   +6.67%  128,000        │  HPG   -3.2%   25,400         │
│  VNM   +4.12%   76,500        │  MSN   -2.8%   62,100         │
│  ...                           │  ...                           │
├────────────────────────────────┼────────────────────────────────┤
│  Volume Leaders (table)        │  Mini Charts (sparklines)      │
│  VCB   2,345,678               │  ┌─────────┐ ┌─────────┐     │
│  HPG   1,890,123               │  │ VCB 30d │ │ BTC 30d │     │
│  ...                           │  └─────────┘ └─────────┘     │
└────────────────────────────────┴────────────────────────────────┘
```

**Data sources:**
- Live Ticker Bar: WebSocket subscription to `*` (all symbols)
- Top Gainers / Losers / Volume: `GET /api/v1/digest?date=today`
- Mini Charts: `GET /api/v1/ohlcv?symbol=X&from=30d-ago` for each pinned symbol

### 6.4 Symbol detail page (`/symbol/:symbol`)

Deep-dive view for a single symbol.

**Layout:**

```
┌─────────────────────────────────────────────────────────────────┐
│  Header:  VCB  |  HOSE  |  85,800  +500 (+0.59%)  ● LIVE      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Candlestick Chart (TradingView Lightweight Charts)             │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                                                          │    │
│  │   OHLCV candles + volume bars + SMA overlays             │    │
│  │   Time range selector: 1W | 1M | 3M | 6M | 1Y | ALL    │    │
│  │                                                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
├───────────────────────────┬─────────────────────────────────────┤
│  Technical Indicators     │  Price Info Panel                    │
│  ┌──────────────────────┐ │  Open:    85,000                    │
│  │ RSI(14) chart        │ │  High:    86,200                    │
│  └──────────────────────┘ │  Low:     84,500                    │
│  ┌──────────────────────┐ │  Close:   85,800                    │
│  │ MACD chart           │ │  Volume:  2,345,678                 │
│  └──────────────────────┘ │                                     │
│  ┌──────────────────────┐ │  SMA20:   85,100                    │
│  │ Bollinger Bands      │ │  SMA50:   84,200                    │
│  │ (overlay on main)    │ │  SMA200:  82,500                    │
│  └──────────────────────┘ │  RSI14:   58.3                      │
│                           │  MACD:    120.5                      │
└───────────────────────────┴─────────────────────────────────────┘
```

**Data sources:**
- Header live price: WebSocket subscription to `[symbol]`
- Candlestick chart: `GET /api/v1/ohlcv?symbol=VCB&from=...&to=...`
- Indicator sub-charts: `GET /api/v1/indicators?symbol=VCB&from=...&to=...`
- Live candle update: on each WebSocket tick, update the last candle in the chart

**TradingView Lightweight Charts integration:**
- Use `createChart()` with `addCandlestickSeries()` for OHLCV
- Use `addHistogramSeries()` for volume
- Use `addLineSeries()` for SMA overlays on the main pane
- Create separate panes for RSI and MACD
- On WebSocket tick: call `series.update()` to update the most recent candle in real time

### 6.5 Screener page (`/screener`)

**Data source:** `GET /api/v1/screener?year=2026&week=21`

Sortable, filterable table using TanStack Table:

| Symbol | P/E | P/B | ROE (%) | EPS | D/E | Current Ratio |
|---|---|---|---|---|---|---|
| VCB | 14.2 | 2.1 | 22.5 | 6,200 | 0.8 | 1.4 |
| FPT | 18.5 | 3.0 | 19.8 | 4,800 | 1.2 | 1.6 |

Week selector dropdown to browse historical screener results.

### 6.6 Digest page (`/digest`)

**Data source:** `GET /api/v1/digest?date=2026-05-25`

Three tabbed sections: Gainers | Losers | Volume. Date picker to browse historical digests.

### 6.7 WebSocket state management (Zustand)

```typescript
interface TickerStore {
  prices: Record<string, PriceTick>;     // symbol → latest tick
  connected: boolean;
  subscribe: (symbols: string[]) => void;
  unsubscribe: (symbols: string[]) => void;
}

// The store manages the WebSocket lifecycle:
// - connects on first subscribe()
// - reconnects with exponential backoff on disconnect
// - updates prices map on every tick
// - components use useSyncExternalStore for zero-copy reads
```

### 6.8 API client layer (TanStack Query)

```typescript
// hooks/useOHLCV.ts
export function useOHLCV(symbol: string, from: string, to: string) {
  return useQuery({
    queryKey: ['ohlcv', symbol, from, to],
    queryFn: () => fetchOHLCV(symbol, from, to),
    staleTime: 5 * 60 * 1000,     // 5 min — OHLCV is immutable for past days
    gcTime: 30 * 60 * 1000,
  });
}

// hooks/useDigest.ts
export function useDigest(date: string) {
  return useQuery({
    queryKey: ['digest', date],
    queryFn: () => fetchDigest(date),
    staleTime: Infinity,           // digest for a past day never changes
  });
}
```

### 6.9 Project structure

```
mekong-web/
  public/
    favicon.ico
  src/
    main.tsx
    App.tsx
    routes/
      Dashboard.tsx
      SymbolDetail.tsx
      Screener.tsx
      Digest.tsx
      Settings.tsx
    components/
      layout/
        Navbar.tsx
        Sidebar.tsx
        LiveTickerBar.tsx
      charts/
        CandlestickChart.tsx       # TradingView Lightweight Charts wrapper
        RSIChart.tsx
        MACDChart.tsx
        SparklineChart.tsx
      tables/
        DigestTable.tsx
        ScreenerTable.tsx
        SymbolList.tsx
      common/
        PriceDisplay.tsx           # formatted price with change color
        DateRangePicker.tsx
        LoadingSpinner.tsx
        ErrorBoundary.tsx
    hooks/
      useOHLCV.ts
      useIndicators.ts
      useDigest.ts
      useScreener.ts
      useSymbols.ts
      useSnapshot.ts
    stores/
      tickerStore.ts               # Zustand WebSocket state
    api/
      client.ts                    # base fetch wrapper
      types.ts                     # API response types
    lib/
      format.ts                    # number/date formatting (VND, %, etc.)
      constants.ts
    styles/
      globals.css                  # Tailwind base
  index.html
  vite.config.ts
  tailwind.config.ts
  tsconfig.json
  package.json
  Dockerfile
```

### 6.10 Dockerfile (multi-stage)

The React SPA is built into static files and served by a minimal nginx container. Kong
proxies traffic to this container for non-API paths.

```dockerfile
FROM node:22-alpine AS builder
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
```

The nginx inside `mekong-web` is minimal — it only serves static files and handles SPA
fallback routing. All API routing, CORS, auth, and rate limiting are handled by Kong.

```nginx
server {
    listen 80;
    root /usr/share/nginx/html;
    location / {
        try_files $uri $uri/ /index.html;
    }
}
```

### 6.11 Kong API Gateway configuration

Kong runs in **DB-less mode** with a declarative YAML config checked into `mekong-infra`.
The config file defines all routes, services, and plugins.

**`kong/kong.yml`:**

```yaml
_format_version: "3.0"

services:
  # React SPA (static files via mekong-web nginx)
  - name: web
    url: http://mekong-web:80
    routes:
      - name: web-root
        paths: ["/"]
        strip_path: false

  # REST API
  - name: api
    url: http://mekong-api:8090
    routes:
      - name: api-routes
        paths: ["/api"]
        strip_path: false
    plugins:
      - name: cors
        config:
          origins: ["*"]            # restrict in production
          methods: [GET, POST, PUT, DELETE, OPTIONS]
          headers: [Content-Type, X-API-Key, Authorization]
          max_age: 3600
      # Phase 4: uncomment to enable
      # - name: key-auth
      #   config:
      #     key_names: [X-API-Key]
      # - name: rate-limiting
      #   config:
      #     minute: 100
      #     policy: local

  # WebSocket server
  - name: ws
    url: http://mekong-ws:8091
    routes:
      - name: ws-route
        paths: ["/ws"]
        strip_path: false
        protocols: [http, https, ws, wss]

  # Internal snapshot API — not exposed to browsers
  - name: internal
    url: http://mekong-ws:8091
    routes:
      - name: internal-snapshot
        paths: ["/internal"]
        strip_path: false
    plugins:
      - name: ip-restriction
        config:
          allow: ["172.16.0.0/12"]  # Docker network only
```

**Kong environment (DB-less mode):**

```yaml
environment:
  KONG_DATABASE: "off"
  KONG_DECLARATIVE_CONFIG: /etc/kong/kong.yml
  KONG_PROXY_LISTEN: 0.0.0.0:3002
  KONG_LOG_LEVEL: info
```

---

## 7. Docker Compose Integration

All three services are added to `mekong-infra/docker-compose.yml`.

### 7.1 New services

```yaml
  # ── Data Platform ──────────────────────────────────────────────────────────────

  postgres:
    image: postgres:17-alpine
    container_name: postgres
    restart: unless-stopped
    environment:
      POSTGRES_USER: mekong
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-mekong}
      POSTGRES_DB: mekong_api
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U mekong -d mekong_api"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 10s
    deploy:
      resources:
        limits:
          memory: 512m
        reservations:
          memory: 128m

  mekong-api:
    build:
      context: ${MEKONG_API_DIR:-../mekong-api}
      dockerfile: Dockerfile
    container_name: mekong-api
    restart: unless-stopped
    environment:
      PORT: "8090"
      GIN_MODE: release
      MINIO_ENDPOINT: minio:9000
      MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY:-minioadmin}
      MINIO_SECRET_KEY: ${MINIO_SECRET_KEY:-minioadmin}
      MINIO_ANALYSIS_BUCKET: ${MINIO_ANALYSIS_BUCKET:-market-analysis}
      POSTGRES_URL: postgres://mekong:${POSTGRES_PASSWORD:-mekong}@postgres:5432/mekong_api?sslmode=disable
      LOG_LEVEL: ${LOG_LEVEL:-info}
    depends_on:
      minio:
        condition: service_healthy
      postgres:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8090/api/v1/health"]
      interval: 15s
      timeout: 5s
      retries: 5
      start_period: 10s
    deploy:
      resources:
        limits:
          memory: 512m
        reservations:
          memory: 128m

  mekong-ws:
    build:
      context: ${MEKONG_WS_DIR:-../mekong-ws}
      dockerfile: Dockerfile
    container_name: mekong-ws
    restart: unless-stopped
    environment:
      PORT: "8091"
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      KAFKA_CONSUMER_GROUP: ws-fanout
      MAX_CLIENTS: "1000"
      LOG_LEVEL: ${LOG_LEVEL:-info}
    depends_on:
      kafka:
        condition: service_healthy
    deploy:
      resources:
        limits:
          memory: 256m
        reservations:
          memory: 64m

  mekong-web:
    build:
      context: ${MEKONG_WEB_DIR:-../mekong-web}
      dockerfile: Dockerfile
    container_name: mekong-web
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 128m
        reservations:
          memory: 32m

  kong:
    image: kong:3.9
    container_name: kong
    restart: unless-stopped
    ports:
      - "3002:3002"
    environment:
      KONG_DATABASE: "off"
      KONG_DECLARATIVE_CONFIG: /etc/kong/kong.yml
      KONG_PROXY_LISTEN: 0.0.0.0:3002
      KONG_LOG_LEVEL: info
    volumes:
      - ./kong/kong.yml:/etc/kong/kong.yml:ro
    depends_on:
      mekong-api:
        condition: service_healthy
      mekong-ws:
        condition: service_started
      mekong-web:
        condition: service_started
    healthcheck:
      test: ["CMD", "kong", "health"]
      interval: 15s
      timeout: 5s
      retries: 5
      start_period: 15s
    deploy:
      resources:
        limits:
          memory: 512m
        reservations:
          memory: 128m
```

New volume to add to the `volumes:` section:

```yaml
volumes:
  postgres_data:
```

### 7.2 New Makefile targets

```makefile
build-api: ## Build mekong-api Docker image
	$(COMPOSE) build mekong-api

build-ws: ## Build mekong-ws Docker image
	$(COMPOSE) build mekong-ws

build-web: ## Build mekong-web Docker image
	$(COMPOSE) build mekong-web

platform-up: ## Start data platform (Postgres + API + WS + Web + Kong) → http://localhost:3002
	$(COMPOSE) up -d postgres mekong-api mekong-ws mekong-web kong

platform-down: ## Stop data platform
	$(COMPOSE) stop kong mekong-web mekong-ws mekong-api postgres
```

### 7.3 Port allocation

| Port | Service | Protocol | Exposed to host? |
|---|---|---|---|
| 3002 | Kong (API Gateway) | HTTP | Yes — single entry point |
| 8090 | `mekong-api` (Gin) | HTTP | No — internal only |
| 8091 | `mekong-ws` | WebSocket | No — internal only |
| 80 | `mekong-web` (nginx) | HTTP | No — internal only |
| 5432 | Postgres | TCP | No — internal only |

In production, only port 3002 (Kong) is exposed to the host. All other services communicate
over the Docker network. Kong routes:
- `/` → `mekong-web` (static React SPA)
- `/api/*` → `mekong-api` (Go REST API)
- `/ws` → `mekong-ws` (WebSocket server)

---

## 8. Implementation Plan

This section is the step-by-step build order. Each step lists what to build, what files to
create, how to verify it works, and what must be done before moving on. Steps within a
phase are sequential — each depends on the previous one.

---

### Phase 1 — Go API serving historical data from MinIO

**Prerequisites:** Mekong pipeline running (`make up`), MinIO has OHLCV + indicator +
digest + screener Parquet data from at least one Spark run.

**Goal:** `curl localhost:3002/api/v1/ohlcv?symbol=VCB` returns JSON.

---

#### Step 1.1 — Infrastructure: Postgres + Kong in Docker Compose

**What:** add Postgres and Kong services to `mekong-infra/docker-compose.yml`, create
the Kong declarative config, add `postgres_data` volume.

**Files to create/modify:**
- `mekong-infra/docker-compose.yml` — add `postgres` and `kong` services (see §7.1)
- `mekong-infra/kong/kong.yml` — declarative gateway config (see §6.11)
- `mekong-infra/Makefile` — add `platform-up`, `platform-down` targets

**Verify:**
```bash
cd mekong-infra
docker compose up -d postgres
docker exec postgres pg_isready -U mekong -d mekong_api   # → accepting connections
docker compose down postgres
```

---

#### Step 1.2 — Scaffold `mekong-api` Go project

**What:** create the repo, initialise Go modules, install dependencies, set up the
project directory structure from §4.6.

**Commands:**
```bash
cd ~/Projects/Python/Mekong
mkdir mekong-api && cd mekong-api
git init
go mod init github.com/davidhoang2406/mekong-api
go get github.com/gin-gonic/gin
go get github.com/marcboeker/go-duckdb
go get github.com/jackc/pgx/v5
go get github.com/golang-migrate/migrate/v4
```

**Files to create:**
```
cmd/server/main.go           # parse env → init DuckDB → init PG pool → start Gin
internal/config/config.go    # struct with all env vars, Validate() method
internal/app/app.go          # App struct holding *sql.DB (DuckDB) + *pgxpool.Pool
internal/app/router.go       # SetupRouter() → gin.Engine with route groups
internal/middleware/logging.go
internal/store/duckdb.go     # InitDuckDB() — open connection, run httpfs setup
internal/store/postgres.go   # InitPostgres() — open pool, run migrations
internal/store/cache.go      # TTL cache (sync.RWMutex + map)
internal/model/              # Go structs with json tags (one file per domain)
Dockerfile                   # multi-stage build (see §4.7)
.gitignore
```

**Verify:**
```bash
go build ./cmd/server         # compiles without error
go vet ./...                  # no issues
```

---

#### Step 1.3 — DuckDB + MinIO integration

**What:** implement `internal/store/duckdb.go` that opens a DuckDB connection, loads the
`httpfs` extension, configures S3 credentials, and exposes parameterised query methods.

**Key functions:**
```go
func InitDuckDB(cfg config.Config) (*sql.DB, error)        // load httpfs, set S3 creds
func QueryOHLCV(db *sql.DB, symbol, from, to string) ([]model.OHLCVBar, error)
func QueryIndicators(db *sql.DB, symbol, from, to string) ([]model.IndicatorRow, error)
func QuerySymbols(db *sql.DB, assetClass string) ([]model.SymbolInfo, error)
```

**Verify:**
```bash
# Start MinIO, ensure it has data
cd mekong-infra && docker compose up -d minio
# Run a Go integration test that queries real Parquet from MinIO
cd mekong-api && go test -v -tags integration ./internal/store/...
```

---

#### Step 1.4 — Postgres integration + migrations

**What:** implement `internal/store/postgres.go` with `pgxpool` init and migration runner.
Write the initial SQL migrations.

**Files to create:**
```
internal/store/postgres.go
migrations/000001_create_users.up.sql
migrations/000001_create_users.down.sql
migrations/000002_create_api_keys.up.sql
migrations/000002_create_api_keys.down.sql
migrations/000003_create_watchlists.up.sql
migrations/000003_create_watchlists.down.sql
```

Migration SQL is defined in §4.8 of this document.

**Verify:**
```bash
cd mekong-infra && docker compose up -d postgres
cd mekong-api && go test -v -tags integration ./internal/store/... -run TestPostgres
# Verify tables exist:
docker exec postgres psql -U mekong -d mekong_api -c '\dt'
# → users, api_keys, watchlists tables listed
```

---

#### Step 1.5 — Core API handlers (OHLCV, indicators, symbols)

**What:** implement Gin handlers for the three most important endpoints. Wire them into
the router. Expose `/api/v1/health` as the first handler.

**Files to create:**
```
internal/handler/health.go       # returns DuckDB + Postgres + MinIO status
internal/handler/ohlcv.go        # binds query params → calls store → c.JSON()
internal/handler/indicators.go
internal/handler/symbols.go
```

**Handler pattern (every handler follows this):**
```go
func (a *App) GetOHLCV(c *gin.Context) {
    // 1. Bind + validate query params
    // 2. Call store function (DuckDB query)
    // 3. Return JSON or abort with error
}
```

**Verify:**
```bash
cd mekong-api && go run ./cmd/server &
curl -s localhost:8090/api/v1/health | jq .
# → {"status":"ok","duckdb":"connected","postgres":"connected",...}

curl -s 'localhost:8090/api/v1/symbols' | jq '.symbols | length'
# → number > 0

curl -s 'localhost:8090/api/v1/ohlcv?symbol=VCB&from=2026-05-01&to=2026-05-25' | jq '.bars[0]'
# → { "time": "...", "open": ..., "close": ... }

curl -s 'localhost:8090/api/v1/indicators?symbol=VCB&from=2026-05-20' | jq '.indicators[0]'
# → { "sma20": ..., "rsi14": ..., "macd": ... }
```

---

#### Step 1.6 — Digest + screener handlers

**What:** add the remaining two analytical endpoints.

**Files to create:**
```
internal/handler/digest.go
internal/handler/screener.go
```

**Verify:**
```bash
curl -s 'localhost:8090/api/v1/digest?date=2026-05-20' | jq '.digest | length'
# → 30 (10 gainers + 10 losers + 10 volume)

curl -s 'localhost:8090/api/v1/screener?year=2026&week=21' | jq '.results | length'
# → number > 0
```

---

#### Step 1.7 — Dockerise `mekong-api` + Kong routing

**What:** build the Docker image, add the service to `docker-compose.yml`, configure Kong
to route `/api/*` to `mekong-api:8090`.

**Files to modify:**
- `mekong-infra/docker-compose.yml` — add `mekong-api` service
- `mekong-infra/kong/kong.yml` — add API service + route + CORS plugin
- `mekong-infra/Makefile` — add `build-api` target

**Verify:**
```bash
cd mekong-infra
make build-api
docker compose up -d postgres mekong-api kong
curl -s localhost:3002/api/v1/health | jq .
# → routed through Kong to mekong-api
curl -s 'localhost:3002/api/v1/ohlcv?symbol=VCB&from=2026-05-20' | jq .
# → JSON response with OHLCV data
```

**Gate:** all six API endpoints return valid JSON through Kong on port 3002. CI passes
(`go test ./... && go vet ./...`).

---

### Phase 2 — React dashboard with historical charts

**Prerequisites:** Phase 1 complete. `curl localhost:3002/api/v1/ohlcv?symbol=VCB` works.

**Goal:** open `http://localhost:3002` in a browser and see candlestick charts.

---

#### Step 2.1 — Scaffold `mekong-web` React project

**Commands:**
```bash
cd ~/Projects/Python/Mekong
npm create vite@latest mekong-web -- --template react-ts
cd mekong-web
npm install
npm install @tanstack/react-query react-router-dom zustand
npm install lightweight-charts recharts
npm install -D tailwindcss @tailwindcss/vite
npx shadcn@latest init
```

**Files to create:**
```
src/main.tsx                         # QueryClientProvider + RouterProvider
src/App.tsx                          # layout with Navbar + Outlet
src/routes/Dashboard.tsx             # placeholder
src/routes/SymbolDetail.tsx          # placeholder
src/routes/Screener.tsx              # placeholder
src/routes/Digest.tsx                # placeholder
src/api/client.ts                    # base fetch wrapper (baseURL from env)
src/api/types.ts                     # TypeScript interfaces matching API responses
vite.config.ts                       # proxy /api → localhost:8090, /ws → localhost:8091
```

**Verify:**
```bash
npm run dev
# Open http://localhost:5173 → see placeholder pages
# Verify Vite proxy:
curl -s localhost:5173/api/v1/health
# → proxied to mekong-api
```

---

#### Step 2.2 — API hooks (TanStack Query)

**What:** create custom hooks that fetch data from the API and manage caching.

**Files to create:**
```
src/hooks/useSymbols.ts              # GET /api/v1/symbols
src/hooks/useOHLCV.ts                # GET /api/v1/ohlcv?symbol=X&from=Y&to=Z
src/hooks/useIndicators.ts           # GET /api/v1/indicators?symbol=X&from=Y&to=Z
src/hooks/useDigest.ts               # GET /api/v1/digest?date=X
src/hooks/useScreener.ts             # GET /api/v1/screener?year=X&week=Y
```

**Verify:** write a temporary test component that calls `useOHLCV("VCB", ...)` and
renders the response as `<pre>{JSON.stringify(data)}</pre>`. Confirm data appears.

---

#### Step 2.3 — Candlestick chart component

**What:** wrap TradingView Lightweight Charts in a React component.

**Files to create:**
```
src/components/charts/CandlestickChart.tsx
src/components/charts/VolumeChart.tsx         # histogram below candles
src/lib/format.ts                             # formatVND(), formatPercent(), formatDate()
```

**Key behaviour:**
- Accept `bars: OHLCVBar[]` as props → render candlestick + volume.
- Time range selector (1W, 1M, 3M, 6M, 1Y, ALL) that adjusts the `from` param in
  `useOHLCV` and re-fetches.
- `ResizeObserver` to handle container resizing.

**Verify:** navigate to `/symbol/VCB` → candlestick chart renders with real data from
MinIO. Switching time ranges re-fetches and re-renders.

---

#### Step 2.4 — Symbol detail page

**What:** assemble the full symbol detail page with chart, indicator sub-charts, and
price info panel.

**Files to create/modify:**
```
src/routes/SymbolDetail.tsx          # full layout (see §6.4 wireframe)
src/components/charts/RSIChart.tsx
src/components/charts/MACDChart.tsx
src/components/common/PriceDisplay.tsx
```

**Verify:** `/symbol/VCB` shows candlestick chart, RSI sub-chart, MACD sub-chart, and
a price info panel with SMA/RSI/MACD values from the indicators endpoint.

---

#### Step 2.5 — Dashboard, digest, and screener pages

**Files to create:**
```
src/routes/Dashboard.tsx             # symbol list + mini sparkline charts
src/routes/Digest.tsx                # tabbed gainers/losers/volume + date picker
src/routes/Screener.tsx              # sortable table + week selector
src/components/tables/DigestTable.tsx
src/components/tables/ScreenerTable.tsx
src/components/tables/SymbolList.tsx
src/components/charts/SparklineChart.tsx
src/components/common/DateRangePicker.tsx
```

**Verify:**
- `/` → shows symbol list, each row links to `/symbol/:symbol`
- `/digest` → shows today's gainers/losers/volume, date picker changes data
- `/screener` → shows this week's screener results, sortable columns

---

#### Step 2.6 — Dockerise `mekong-web` + Kong static routing

**Files to create/modify:**
- `mekong-web/Dockerfile` — multi-stage build (see §6.10)
- `mekong-web/nginx.conf` — minimal SPA fallback config
- `mekong-infra/docker-compose.yml` — add `mekong-web` service
- `mekong-infra/kong/kong.yml` — add web service + `/` route
- `mekong-infra/Makefile` — add `build-web` target

**Verify:**
```bash
cd mekong-infra
make build-web
docker compose up -d postgres mekong-api mekong-web kong
# Open http://localhost:3002 → React dashboard loads
# Navigate to /symbol/VCB → chart renders with real data
# Navigate to /digest → table renders
```

**Gate:** all four pages render correctly with real data from MinIO, served through Kong.
No console errors. CI passes (`npm run lint && npm run type-check && npm run build`).

---

### Phase 3 — WebSocket server + live data

**Prerequisites:** Phase 2 complete. Dashboard renders historical data.

**Goal:** live price ticks stream into the dashboard in real time.

---

#### Step 3.1 — Scaffold `mekong-ws` Go project

**Commands:**
```bash
cd ~/Projects/Python/Mekong
mkdir mekong-ws && cd mekong-ws
git init
go mod init github.com/davidhoang2406/mekong-ws
go get github.com/gorilla/websocket
go get github.com/segmentio/kafka-go
```

**Files to create:**
```
cmd/server/main.go
internal/config/config.go
internal/hub/hub.go
internal/hub/client.go
internal/kafka/consumer.go
internal/snapshot/cache.go
internal/model/tick.go
internal/model/protocol.go
Dockerfile
.gitignore
```

---

#### Step 3.2 — Kafka consumer goroutine

**What:** implement `internal/kafka/consumer.go` — connects to Kafka, subscribes to both
price topics, deserialises JSON envelopes into `Tick` structs, sends to an output channel.

**Key behaviour:**
- Consumer group: `ws-fanout`
- Topics: `stock.price.realtime`, `crypto.price.realtime`
- On each message: parse JSON → extract `symbol`, `exchange`, `price`, `change`,
  `pct_change`, `volume`, `bid`, `ask`, `timestamp` → send `Tick` to channel

**Verify:**
```bash
# Run with producers active
cd mekong-ws && go run ./cmd/server
# Logs should show ticks being consumed from Kafka
# e.g. "consumed tick symbol=VCB price=85800"
```

---

#### Step 3.3 — Hub + client goroutines

**What:** implement the hub/spoke fan-out pattern (§5.2).

**Key behaviour:**
- Hub maintains `subscriptions map[string]map[*Client]struct{}`
- On tick: fan out to subscribers of that symbol + `*` wildcard subscribers
- Client read goroutine: parse subscribe/unsubscribe JSON
- Client write goroutine: drain outbound channel, drop on full, disconnect after
  `DROP_THRESHOLD` consecutive drops

**Verify:**
```bash
cd mekong-ws && go run ./cmd/server &
# Connect with wscat:
npx wscat -c ws://localhost:8091/ws
> {"action":"subscribe","symbols":["*"]}
# → should receive tick JSON frames every few seconds
> {"action":"unsubscribe","symbols":["*"]}
> {"action":"subscribe","symbols":["VCB"]}
# → only VCB ticks
```

---

#### Step 3.4 — Snapshot cache + internal HTTP endpoint

**What:** implement `internal/snapshot/cache.go` — updates `lastPrice[symbol]` on every
tick. Expose `GET /internal/snapshot?symbol=X` as an HTTP endpoint on the same server.

**Verify:**
```bash
curl -s 'localhost:8091/internal/snapshot?symbol=VCB' | jq .
# → latest VCB tick data
```

---

#### Step 3.5 — Dockerise `mekong-ws` + Kong WebSocket routing

**Files to modify:**
- `mekong-infra/docker-compose.yml` — add `mekong-ws` service
- `mekong-infra/kong/kong.yml` — add ws service + `/ws` route with WebSocket protocols
- `mekong-infra/Makefile` — add `build-ws` target

**Verify:**
```bash
cd mekong-infra
make build-ws
docker compose up -d kafka mekong-ws kong
npx wscat -c ws://localhost:3002/ws
> {"action":"subscribe","symbols":["*"]}
# → ticks arrive through Kong
```

---

#### Step 3.6 — Snapshot endpoint in `mekong-api`

**What:** add `GET /api/v1/snapshot?symbol=X` to `mekong-api` that makes an internal HTTP
call to `mekong-ws:8091/internal/snapshot?symbol=X` and returns the result.

**Files to create:**
```
mekong-api/internal/handler/snapshot.go
```

**Verify:**
```bash
curl -s 'localhost:3002/api/v1/snapshot?symbol=VCB' | jq .
# → latest price via Kong → mekong-api → mekong-ws
```

---

#### Step 3.7 — Live UI integration

**What:** add WebSocket state management and live components to the React dashboard.

**Files to create:**
```
src/stores/tickerStore.ts               # Zustand store (see §6.7)
src/components/layout/LiveTickerBar.tsx  # horizontal scroll of live prices
```

**Files to modify:**
```
src/routes/SymbolDetail.tsx             # subscribe to symbol, update last candle live
src/routes/Dashboard.tsx                # add LiveTickerBar at top
src/components/charts/CandlestickChart.tsx  # accept live tick, call series.update()
```

**Key behaviour:**
- `tickerStore.ts` connects on first `subscribe()`, reconnects with exponential backoff
- `LiveTickerBar` subscribes to `*`, shows all prices in horizontal scroll
- `SymbolDetail` subscribes to `[symbol]`, updates header price + last candle in chart
- Green/red colour on price change

**Verify:**
- Open `http://localhost:3002` → LiveTickerBar scrolls with live prices
- Open `/symbol/VCB` → header price updates on each tick, last candle updates in chart
- Kill and restart `mekong-ws` → "Reconnecting..." indicator appears, then reconnects

**Gate:** live data flows end-to-end: Kafka producer → mekong-ws → Kong → browser.
Historical and live views coexist on the same page. All CI pipelines pass.

---

### Phase 4 — Polish, error handling, UX

**Prerequisites:** Phase 3 complete. Dashboard shows both historical and live data.

**Goal:** production-quality user experience.

---

#### Step 4.1 — Error handling + loading states

**Files to create/modify:**
```
src/components/common/ErrorBoundary.tsx     # catches render errors per page
src/components/common/LoadingSpinner.tsx
src/components/common/EmptyState.tsx         # "No data for this date range"
src/components/common/SkeletonChart.tsx      # placeholder while chart loads
src/components/common/SkeletonTable.tsx
```

**Modify all route components** to wrap data-fetching sections with loading/error states:
```tsx
const { data, isLoading, error } = useOHLCV(symbol, from, to);
if (isLoading) return <SkeletonChart />;
if (error) return <ErrorBoundary error={error} />;
if (!data?.bars.length) return <EmptyState message="No OHLCV data" />;
```

**TanStack Query retry config** (set globally in `main.tsx`):
```tsx
const queryClient = new QueryClient({
  defaultOptions: {
    queries: { retry: 3, retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 30000) },
  },
});
```

---

#### Step 4.2 — Responsive design + theming

- Add Tailwind responsive breakpoints to all layout components (`sm:`, `md:`, `lg:`)
- Stack panels vertically on mobile (< 768px)
- Add dark/light theme toggle using Tailwind `dark:` classes + `localStorage` persistence
- Implement `ResizeObserver` in chart components for container-aware resizing

---

#### Step 4.3 — Performance optimisation

- Add `TanStack Virtual` to `ScreenerTable` and `DigestTable` for virtualised rows
- Memoise chart components with `React.memo()` to prevent re-renders on unrelated state
- Add `limit` + `offset` pagination to `/api/v1/ohlcv` for large date ranges (> 1 year)
- Add in-memory DuckDB result cache for the `/api/v1/symbols` query (full scan, cache 1h)

---

#### Step 4.4 — WebSocket reconnection UX

**Files to modify:**
```
src/stores/tickerStore.ts       # add connectionState: 'connected' | 'reconnecting' | 'disconnected'
src/components/layout/Navbar.tsx # show connection status indicator
```

- Green dot when connected, yellow pulsing dot when reconnecting, red when disconnected
- Reconnect with exponential backoff: 1s, 2s, 4s, 8s, 16s, cap at 30s

**Gate:** all pages handle loading, error, and empty states. Dark mode works. Mobile layout
is usable. Charts resize correctly. No uncaught exceptions in console.

---

### Phase 5 — Auth, rate limiting, multi-user

**Prerequisites:** Phase 4 complete. Dashboard is polished and stable.

**Goal:** multi-user platform with API keys and usage controls.

---

#### Step 5.1 — Kong auth + rate limiting plugins

**Files to modify:**
- `mekong-infra/kong/kong.yml` — enable `key-auth` and `rate-limiting` plugins on the
  API service (currently commented out in the config)

**Verify:**
```bash
# Without API key:
curl -s localhost:3002/api/v1/ohlcv?symbol=VCB
# → 401 Unauthorized (Kong rejects)

# With valid key:
curl -s -H 'X-API-Key: test-key-123' localhost:3002/api/v1/ohlcv?symbol=VCB
# → 200 OK with data
```

Note: Kong in DB-less mode manages consumers via the declarative config. For dynamic
consumer management (self-service key creation), switch Kong to database-backed mode
using the same Postgres instance.

---

#### Step 5.2 — User registration + API key issuance

**Files to create:**
```
mekong-api/internal/handler/user.go        # POST /api/v1/users (register)
mekong-api/internal/handler/apikey.go      # POST /api/v1/users/:id/keys (issue key)
mekong-api/internal/handler/auth.go        # POST /api/v1/auth/login (JWT issuance)
```

**Key behaviour:**
- `POST /api/v1/users` — creates user in Postgres, returns user ID
- `POST /api/v1/users/:id/keys` — generates random API key, stores bcrypt hash in
  `api_keys` table, returns the raw key (one-time display)
- `POST /api/v1/auth/login` — validates email + password, returns JWT
- Kong `jwt` plugin validates JWT on subsequent requests; `X-Consumer-Username` header
  identifies the user to downstream handlers

---

#### Step 5.3 — Watchlists (Postgres CRUD)

**Files to create:**
```
mekong-api/internal/handler/watchlist.go    # CRUD /api/v1/watchlists
```

**Endpoints:**
```
GET    /api/v1/watchlists           → list user's watchlists
POST   /api/v1/watchlists           → create watchlist
PUT    /api/v1/watchlists/:id       → update symbols in watchlist
DELETE /api/v1/watchlists/:id       → delete watchlist
```

User identity comes from `X-Consumer-Username` header (injected by Kong after JWT
validation).

---

#### Step 5.4 — Frontend auth flow

**Files to create:**
```
src/routes/Login.tsx
src/routes/Register.tsx
src/stores/authStore.ts              # JWT token management + localStorage
src/api/client.ts                    # add Authorization header from authStore
src/components/layout/UserMenu.tsx   # profile dropdown, logout
```

**Key behaviour:**
- Login page → `POST /api/v1/auth/login` → store JWT in `authStore`
- `client.ts` attaches `Authorization: Bearer <jwt>` to all API requests
- Protected routes redirect to `/login` if no JWT
- Watchlist page shows user's saved watchlists from Postgres

---

#### Step 5.5 — API documentation

**What:** add Swagger/OpenAPI documentation to `mekong-api`.

```bash
cd mekong-api
go install github.com/swaggo/swag/cmd/swag@latest
swag init -g cmd/server/main.go
```

Add `swaggo/gin-swagger` middleware to serve Swagger UI at `/api/docs`.

**Verify:**
```bash
curl -s localhost:3002/api/docs/index.html
# → Swagger UI loads with all endpoints documented
```

**Gate:** users can register, obtain API keys, authenticate, save watchlists, and
access rate-limited endpoints. Swagger UI documents all public endpoints.

---

### Phase summary

| Phase | Duration | Deliverable | Depends on |
|---|---|---|---|
| 1. Go API | 1-2 weeks | `curl /api/v1/ohlcv` returns JSON through Kong | Existing pipeline |
| 2. React dashboard | 2-3 weeks | Browser shows charts + tables with historical data | Phase 1 |
| 3. WebSocket + live | 1-2 weeks | Live price ticks stream into the dashboard | Phase 2 |
| 4. Polish + UX | 1-2 weeks | Error handling, loading states, dark mode, mobile | Phase 3 |
| 5. Auth + multi-user | 2-3 weeks | API keys, JWT, watchlists, rate limiting | Phase 4 |
| **Total** | **7-12 weeks** | **Full data platform** | |

---

## 9. Testing Strategy

### 9.1 `mekong-api` (Go)

| Level | What | How |
|---|---|---|
| Unit | Query builders, cache logic, response serialisation | `go test ./internal/...` with table-driven tests |
| Integration | DuckDB + local Parquet files | Embed test Parquet fixtures; `go test -tags integration` |
| E2E | Full API against MinIO | Docker Compose test profile; `go test -tags e2e` |

### 9.2 `mekong-ws` (Go)

| Level | What | How |
|---|---|---|
| Unit | Hub subscription logic, message serialisation | `go test ./internal/hub/...` |
| Integration | Kafka consumer → Hub → mock client | Testcontainers with Kafka image |
| Load | 500 concurrent WebSocket clients | `k6` with WebSocket protocol |

### 9.3 `mekong-web` (React)

| Level | What | How |
|---|---|---|
| Unit | Hooks, formatters, store logic | Vitest + React Testing Library |
| Component | Chart rendering, table sorting | Vitest + jsdom (mock API responses) |
| E2E | Full user flows | Playwright against a running Docker Compose stack |

### 9.4 CI pipeline

Add to each repo's `.github/workflows/ci.yml`:

**mekong-api / mekong-ws:**
```yaml
- uses: actions/setup-go@v5
  with:
    go-version: "1.23"
- run: go test ./...
- run: go vet ./...
- run: golangci-lint run
```

**mekong-web:**
```yaml
- uses: actions/setup-node@v4
  with:
    node-version: "22"
- run: npm ci
- run: npm run lint
- run: npm run type-check
- run: npm test
- run: npm run build
```

---

## 10. Performance Considerations

### 10.1 DuckDB query latency targets

| Query type | Expected latency | Notes |
|---|---|---|
| Single symbol, 30 days OHLCV | < 50 ms | Partition pruning on year/month/day |
| Single symbol, 1 year OHLCV | < 200 ms | ~365 partitions, columnar scan |
| Full symbol list | < 500 ms | Full scan, cached for 1 hour |
| Digest for one day | < 30 ms | Single partition |
| Screener for one week | < 30 ms | Single partition |

If latency exceeds these targets, consider:
- Pre-materialising symbol metadata in a DuckDB in-memory table on startup.
- Using DuckDB's `CREATE VIEW` with Parquet paths to avoid re-parsing on each query.
- Adding a Parquet metadata cache to avoid S3 `HEAD` calls on every query.

### 10.2 WebSocket throughput

With 2 price topics x ~50 symbols x ~1 tick/30s (stock) + ~20 ticks/s (crypto), the
aggregate tick rate is approximately **25 ticks/second** peak. The Hub can handle this
easily — Go channel fan-out benchmarks at millions of messages/second.

The bottleneck is per-client write throughput. At 25 ticks/s with a `*` subscription,
each client receives ~25 JSON frames/second (~2.5 KB/s). Comfortable for any modern
browser WebSocket implementation.

### 10.3 Memory footprint

| Service | Expected | Limit |
|---|---|---|
| `mekong-api` | ~100 MB (DuckDB + PG pool + cache) | 512 MB |
| `mekong-ws` | ~50 MB + ~10 KB per client | 256 MB |
| `mekong-web` (nginx) | ~20 MB | 128 MB |
| `kong` | ~150 MB (OpenResty + plugins) | 512 MB |
| `postgres` | ~100 MB (small dataset) | 512 MB |

---

## 11. Deployment Topology

### 11.1 Local development

```
mekong-web (Vite dev server, :5173)
    ├── /api/* → mekong-api (:8090)   # Vite proxy
    └── /ws   → mekong-ws  (:8091)   # Vite proxy

# vite.config.ts
export default defineConfig({
  server: {
    proxy: {
      '/api': 'http://localhost:8090',
      '/ws': { target: 'ws://localhost:8091', ws: true },
    },
  },
});
```

Developers run `npm run dev` for the React app (hot reload) while the Go services and
infrastructure run in Docker. No nginx needed in dev.

### 11.2 Production (Docker Compose)

All services run as Docker containers behind Kong. Only port 3002 (Kong) is published
to the host. Internal service communication happens on the Docker network.

### 11.3 Future: cloud deployment

The architecture maps to cloud services:

| Local | Cloud equivalent |
|---|---|
| MinIO | AWS S3 / GCS |
| Kafka | AWS MSK / Confluent Cloud |
| DuckDB reading S3 | DuckDB reading S3 (same code, different endpoint) |
| Postgres | AWS RDS / Cloud SQL |
| Docker Compose | Kubernetes / ECS / Cloud Run |
| Kong (self-hosted) | Kong Gateway (managed) / AWS API Gateway / Cloud Endpoints |

The Go services need zero code changes — only env var reconfiguration.

---

## 12. Security Considerations

| Concern | Mitigation |
|---|---|
| MinIO credentials in API server | Env vars only; never in code or Docker image layers |
| Postgres credentials | Env vars; never commit `.env` files (enforced by `.gitignore`) |
| WebSocket abuse (connection flooding) | `MAX_CLIENTS` hard cap; close idle connections after 5 min of no subscriptions |
| CORS | Kong `cors` plugin; restrict to specific origins in production (not `*`) |
| SQL injection via DuckDB | Parameterised queries only; no string concatenation |
| SQL injection via Postgres | `pgx` parameterised queries; no raw string interpolation |
| XSS | React's default JSX escaping; Content-Security-Policy header via Kong `response-transformer` |
| Rate limiting | Kong `rate-limiting` plugin (Phase 4); per-consumer or per-IP |
| Auth | Kong `key-auth` + `jwt` plugins (Phase 4); API keys hashed in Postgres |
| HTTPS | Terminate TLS at Kong (or cloud LB); not needed for local dev |
| Internal endpoints | Kong `ip-restriction` plugin blocks external access to `/internal/` |

---

## 13. Open Questions

1. **Symbol search.** Should the dashboard support fuzzy search across symbols (e.g. typing
   "VC" shows "VCB", "VCI", "VCS")? If yes, the API needs a dedicated search endpoint or
   the symbol list should be loaded client-side and filtered in the browser.

2. **Historical depth.** How far back should the API serve data? The OHLCV dataset starts
   from when the pipeline was first deployed. If pre-pipeline historical data is needed,
   a backfill from vnstock/CCXT historical APIs would be required.

3. **Alerts in the UI.** Should the dashboard show Flink-generated alerts (price spikes,
   volatility bursts)? This would require a new Kafka topic for alerts and a corresponding
   WebSocket subscription channel.

4. **Multi-language.** Should the UI support Vietnamese? If yes, add `react-i18next` in
   Phase 3 and extract all user-facing strings.

5. **Mobile app.** Is a mobile app in scope? If yes, the REST API and WebSocket server are
   already mobile-friendly — only the UI layer would need a React Native or Flutter shell.

6. **API monetisation.** If the API is offered to third parties, Kong already provides
   rate limiting and consumer management. Add usage metering via Kong's `prometheus`
   plugin + a billing service, or upgrade to Kong Enterprise for built-in analytics.

---

## 14. Repository Map (Updated)

| Repo | Language | Status | Purpose |
|---|---|---|---|
| `mekong-infra` | Make/Docker | Active | Docker Compose, Kong config, infra scripts |
| `mekong-data-models` | Python | Active | Shared schemas, Avro specs |
| `mekong-kafka` | Python | Active | Producers + storage consumer |
| `mekong-jobs` | Python | Active | Flink + Spark jobs |
| `mekong-notebooks` | Python | Active | Jupyter exploration |
| `mekong-dagster` | Python | Active | Orchestration |
| `mekong-api` | Go | **New** | REST API (Gin + DuckDB + Postgres) |
| `mekong-ws` | Go | **New** | WebSocket server (Kafka fan-out) |
| `mekong-web` | TypeScript | **New** | React dashboard |

Infrastructure services added to `mekong-infra/docker-compose.yml`:

| Service | Image | Role |
|---|---|---|
| `kong` | `kong:3.9` | API Gateway — routing, CORS, auth, rate limiting |
| `postgres` | `postgres:17-alpine` | Application state — users, API keys, watchlists |
