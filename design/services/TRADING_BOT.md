# Design Document — Mekong Trading Bot

This document covers the design space for adding automated trade execution to the Mekong
platform. Three architectural options are evaluated in depth. A recommended phased approach
is given at the end.

---

## 1. Context and Goals

### 1.1 What the platform already provides

| Layer | What exists today |
|---|---|
| Raw prices | `stock.price.realtime` + `crypto.price.realtime` Kafka topics (30 s / 5 s cadence) |
| Raw storage | MinIO `market-data` bucket — Avro snapshots, Hive-partitioned by day |
| OHLCV bars | MinIO `market-analysis` — Spark-computed Parquet, daily partitions |
| Indicators | Spark jobs computing RSI, MACD, Bollinger Bands, MA (in `mekong-jobs`) |
| Screener | Weekly Dagster asset scoring stocks by fundamentals + momentum |
| Orchestration | Dagster asset graph, daily schedule, sensors, Telegram alerts |
| Stream processing | Flink cluster with S3A checkpointing |
| Price producers | `stock-price-producer` (vnstock, 30 s) and `crypto-price-producer` (CCXT/Binance, 5 s) |

### 1.2 Goals

1. Generate trade signals from existing market data.
2. Execute orders automatically on supported venues.
3. Track open positions and realised P&L.
4. Enforce risk guardrails (max drawdown, position limits, daily loss cap).
5. Support paper-trading mode before going live.

### 1.3 Execution venue constraints

**Crypto (Binance via CCXT):**
- CCXT already in the stack for price reading; the same library handles order placement.
- Market, limit, and stop-loss orders supported.
- No additional broker agreements required.
- Sub-second order round-trips achievable.

**Vietnamese equities (HOSE/HNX via vnstock):**
- vnstock is a *read-only* market-data library; it does not provide an order API.
- Live execution requires a separate integration with a Vietnamese broker REST API
  (SSI FastConnect, VPS Securities API, VCBS, or similar).
- Broker APIs are inconsistent in quality, require KYC account setup, and impose
  trading-session constraints (09:00–14:30 ICT, T+2 settlement).
- **Recommendation:** treat Vietnamese equities as signal-only in Phase 1; add execution
  once a broker API integration is proven.

---

## 2. Guiding Constraints

- **No shared mutable state** between the signal layer and the execution layer.
  Signals are written to a durable store (MinIO or Kafka); the executor reads them
  independently. This enables replay and audit.
- **Paper-trading mode is mandatory** before any live capital is deployed. The executor
  must support a `DRY_RUN=true` flag that logs intended orders without submitting them.
- **Risk guardrails are enforced in the executor**, not in the signal generator.
  Signals are opinions; the executor decides whether to act on them.
- **Idempotent signal consumption.** Restarting the executor must not re-submit an
  already-filled order.
- **Full audit trail.** Every signal, every order attempt, every fill, and every
  risk-check rejection must be persisted to MinIO.

---

## 3. Option A — Batch Signal Generation + CCXT Execution

### 3.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  mekong-dagster  (daily schedule)                               │
│                                                                  │
│  OHLCV bars ──► technical_indicators ──► trade_signals ─────┐  │
│  (MinIO)            (Spark job)           (Spark job)        │  │
│                                           writes to          │  │
│                                    market-signals bucket     │  │
└──────────────────────────────────────────────────────────────┼──┘
                                                               │
                                                               ▼
┌──────────────────────────────────────────────────────────────────┐
│  mekong-trader  (long-running service)                           │
│                                                                  │
│  SignalPoller ──► RiskEngine ──► OrderManager ──► CCXTExecutor  │
│  (reads MinIO)    (guardrails)   (dedup, sizing)  (Binance API) │
│       │                │               │                │        │
│       └───────────────────────────────────────────────►│        │
│                                                         │        │
│                                              PositionTracker     │
│                                              (MinIO audit log)   │
└──────────────────────────────────────────────────────────────────┘
```

### 3.2 Signal generation (Spark / Dagster)

A new Spark job in `mekong-jobs` reads the OHLCV Parquet dataset and indicator outputs,
applies a set of rule-based strategies, and writes a signal file to a new MinIO bucket
`market-signals`.

**Signal schema (Parquet, one row per signal):**

| Column | Type | Description |
|---|---|---|
| `signal_id` | string | UUID, used for idempotency |
| `generated_at` | timestamp | When the signal was produced |
| `valid_until` | timestamp | Signal expiry (executor ignores stale signals) |
| `symbol` | string | e.g. `BTC/USDT`, `VCB` |
| `asset_class` | string | `crypto` or `stock` |
| `venue` | string | `binance`, `hose` |
| `direction` | string | `buy` or `sell` |
| `strategy` | string | e.g. `rsi_oversold`, `macd_crossover` |
| `confidence` | float | 0.0–1.0, strategy-specific scoring |
| `suggested_entry` | float | Reference price at signal time |
| `suggested_sl` | float | Suggested stop-loss price |
| `suggested_tp` | float | Suggested take-profit price |
| `partition_date` | date | Hive partition key |

**Example strategies (rule-based, no model required):**

```
RSI Oversold/Overbought
  BUY  when RSI(14) < 30 AND price above 200-day MA
  SELL when RSI(14) > 70 AND price below 200-day MA

MACD Crossover
  BUY  when MACD line crosses above signal line AND histogram turns positive
  SELL when MACD line crosses below signal line AND histogram turns negative

Bollinger Band Reversion
  BUY  when close touches lower band AND RSI < 40
  SELL when close touches upper band AND RSI > 60

MA Crossover (Golden/Death Cross)
  BUY  when 50-day MA crosses above 200-day MA
  SELL when 50-day MA crosses below 200-day MA
```

The Dagster asset graph:

```
price_snapshots (observable)
       │
       ▼
 ohlcv_daily_bars  ◄── daily_market_close schedule
       │
       ▼
 technical_indicators  (existing)
       │
       ▼
 trade_signals  (new asset)        ◄── runs after indicators
       │
       ▼
 signal_dispatch_sensor  (new)     ◄── polls market-signals for un-acted signals
```

### 3.3 Execution service (`mekong-trader`)

A new Python service with four internal components:

**SignalPoller**
- Polls MinIO `market-signals` bucket on a configurable interval (default: 60 s).
- Reads only signals with `valid_until > now` and `signal_id` not in the executed-orders log.
- Emits `SignalEvent` objects to the internal pipeline.

**RiskEngine**
- Evaluates each signal against guardrails before forwarding to the order manager.
- Checks (configurable via environment variables):
  - Daily P&L loss cap: if realised + unrealised loss > `MAX_DAILY_LOSS_USD`, reject all buys.
  - Max open positions: if `open_positions >= MAX_POSITIONS`, reject new buys.
  - Per-asset concentration: if a single asset > `MAX_ASSET_PCT` of portfolio value, reject.
  - Drawdown circuit breaker: if portfolio NAV has fallen > `MAX_DRAWDOWN_PCT` from peak, halt.
  - Asset class filter: ignore signals for venues not in `ENABLED_VENUES`.

**OrderManager**
- Translates a signal into a concrete order (size, type, price).
- Position sizing via fixed-fraction Kelly: `size = (portfolio_value * risk_per_trade) / distance_to_sl`.
- Enforces minimum/maximum order size constraints per venue.
- Checks for an existing open position in the same symbol (prevents doubling up).
- Writes a pending order record to MinIO `trade-audit` before submission.

**CCXTExecutor**
- Wraps `ccxt.binance` (or any other CCXT exchange) for live order submission.
- Supports `DRY_RUN=true` mode: logs the order payload without submitting.
- On submission: writes fill details back to the audit log.
- On failure: retries with exponential backoff (max 3 attempts), then writes a failed-order record.

**PositionTracker**
- Maintains an in-memory position book, bootstrapped from the MinIO audit log on startup.
- Polls CCXT `fetch_open_orders()` and `fetch_balance()` every 30 s to reconcile.
- Computes unrealised P&L for the risk engine.
- Writes daily position snapshots to MinIO `trade-audit`.

### 3.4 New infrastructure

| Component | Where |
|---|---|
| MinIO bucket `market-signals` | `mekong-infra/db/init_minio.py` |
| MinIO bucket `trade-audit` | `mekong-infra/db/init_minio.py` |
| `trade_signals` Spark job | `mekong-jobs/jobs/trade_signals.py` |
| `trade_signals` Dagster asset | `mekong-dagster/dagster_project/assets/signals.py` |
| `mekong-trader` service | New repo + Docker Compose service |
| Dagster `signal_dispatch_sensor` | `mekong-dagster/dagster_project/sensors.py` |

### 3.5 Pros

- **Low complexity.** No new streaming infrastructure; reuses the existing Spark + Dagster pipeline.
- **Auditable.** Signals are written to MinIO before execution; full replay is possible.
- **Incremental.** Can be built in phases: signal generation first, execution later.
- **Decoupled.** The signal generator and executor are independent services; either can be replaced.
- **Backtesting-friendly.** The same signal job can be run over historical partitions to evaluate strategy performance before going live.
- **Fits the existing asset graph.** `trade_signals` is just another downstream Dagster asset.

### 3.6 Cons

- **Latency.** Signals are generated once per day (after market close). Intra-day opportunities are missed entirely.
- **Signal staleness.** A signal generated at 16:00 may no longer be valid by the time the executor checks at 08:00 next morning. The `valid_until` field mitigates this but does not eliminate it.
- **No real-time reaction.** Cannot respond to sudden price moves, news events, or stop-loss triggers in real time. Stop-losses must be placed as exchange-side orders at signal time.
- **Spark overhead for simple rules.** Running a Spark job to apply RSI thresholds is heavy; a pure Python job would suffice for smaller universes.

---

## 4. Option B — Real-Time Flink Signal + Execution

### 4.1 Architecture

```
  stock.price.realtime  ──┐
                           ├──► Flink Signal Job ──► trade.signal (Kafka)
  crypto.price.realtime ──┘    (stateful, windowed)        │
                                                            ▼
                                                   mekong-trader
                                                   KafkaSignalConsumer
                                                         │
                                                   RiskEngine
                                                         │
                                                   OrderManager
                                                         │
                                                   CCXTExecutor
                                                         │
                                              trade-audit (MinIO)
```

### 4.2 Flink signal job design

A new PyFlink job in `mekong-jobs` subscribes to both price topics and maintains stateful
rolling windows to compute indicators and emit signals in real time.

**State design:**

Each symbol maintains a `PriceState` managed by Flink's keyed state backend (RocksDB,
checkpointed to MinIO). The state holds:
- Circular buffer of the last N closes (enough for the longest indicator window, e.g. 200 for MA200).
- Last-emitted signal direction per strategy (to prevent re-emitting the same signal on every tick).
- Cooldown timestamp per strategy (minimum gap between signals for the same symbol).

**Window topology:**

```
Raw price stream
    │
    ├── KeyBy(symbol)
    │       │
    │       ├── ProcessFunction: update PriceState on every tick
    │       │       │
    │       │       ├── if RSI(14) < 30 AND cooldown elapsed → emit BUY signal
    │       │       ├── if MACD crossover detected → emit signal
    │       │       └── if Bollinger touch → emit signal
    │       │
    │       └── Side output: indicator values for monitoring
    │
    └── Sink: trade.signal Kafka topic (Avro schema, same fields as Option A)
```

**New Kafka topic:**

```
Topic: trade.signal
Partitions: 6
Key: symbol
Value: Avro (same signal schema as Option A)
Retention: 24h (signals are time-sensitive; no long-term retention needed)
```

**Cooldown logic:**
To prevent signal storms on choppy price action, each strategy has a configurable minimum
gap before re-emitting for the same symbol:
- RSI strategies: 4-hour cooldown
- MACD crossover: 1-hour cooldown (crossovers are infrequent by nature)
- Bollinger touch: 2-hour cooldown

### 4.3 Execution service changes vs Option A

The `mekong-trader` service replaces the `SignalPoller` (MinIO poll) with a
`KafkaSignalConsumer` (group ID `"trader"`) that reads from `trade.signal`.
All other components (RiskEngine, OrderManager, CCXTExecutor, PositionTracker) are
identical to Option A.

The Kafka consumer enables sub-second signal-to-order latency for crypto, which matters
for momentum and breakout strategies.

### 4.4 Flink state bootstrap problem

The longest window is MA(200), which requires 200 daily closes. On cold start, the Flink
job has no historical state — MA(200) is undefined until 200 ticks have been seen for each
symbol. Two mitigation strategies:

1. **State warm-up from MinIO.** At job startup, a one-time initialisation step reads the
   last 200 OHLCV closes from MinIO Parquet and pre-fills the `PriceState` for each symbol
   before connecting to the live Kafka stream. This adds startup complexity but eliminates
   the warm-up window in production.

2. **Shorten indicator windows.** Use only short-window indicators (RSI(14), MACD(12,26,9))
   for the streaming job. Long-window signals (MA(200)) remain in the batch Dagster pipeline
   (Option A runs in parallel). The streaming job handles intra-day signals; the batch job
   handles trend-following signals.

**Recommendation:** approach 2 (hybrid). Use Flink for short-window intra-day signals and
keep the daily Dagster pipeline for long-window trend signals.

### 4.5 Pros

- **Low latency.** Signals reach the executor within seconds of the price tick that triggered them.
- **Intra-day signals.** Can capture intra-day momentum, reversals, and breakouts that the daily batch pipeline misses entirely.
- **Reuses existing Flink cluster.** No new processing infrastructure.
- **Natural fit for crypto.** Binance data arrives every 5 s; real-time signals + sub-second order submission is achievable.
- **Backpressure-safe.** Flink's flow control prevents the signal generator from overwhelming the executor during volatile markets.

### 4.6 Cons

- **Significantly higher complexity.** Stateful stream processing is harder to develop, test, and debug than a batch Spark job.
- **State bootstrap problem.** Cold start requires either a warm-up delay or a separate initialisation path from MinIO (see §4.4).
- **Harder to backtest.** You cannot trivially replay historical ticks through a stateful Flink job to evaluate strategy performance without a dedicated backtesting harness.
- **Flink operational overhead.** Checkpoint management, state schema evolution, and job restart procedures are non-trivial.
- **Signal duplication risk.** Flink exactly-once semantics require careful sink configuration; at-least-once delivery can trigger duplicate orders if the executor is not fully idempotent.
- **Over-engineered for daily-close strategies.** If the strategy only looks at daily OHLCV data, the streaming infrastructure adds cost without benefit. This option only pays off for sub-hourly signal logic.

---

## 5. Option C — ML-Based Signal Generation

### 5.1 Architecture

```
                    ┌─────────────────────────────────┐
                    │  mekong-ml  (new repo)           │
                    │                                  │
  OHLCV + indicators│  Feature Engineering (Spark)    │
  (MinIO Parquet)  ─►  Model Training (offline)       │
                    │  Model Registry (MinIO)          │
                    │  Inference Job (Spark or Python) │
                    └────────────┬────────────────────┘
                                 │ writes signals
                                 ▼
                         market-signals (MinIO)
                                 │
                                 ▼
                          mekong-trader
                    (same executor as Option A)
```

### 5.2 Model approaches

Three model classes are worth evaluating. They are not mutually exclusive — an ensemble
can combine multiple models.

---

#### 5.2.1 Classification models (predict direction)

**Task:** given a feature vector at time T, predict whether the close price at T+1 (or T+N)
will be higher or lower than the close at T.

**Label construction:**
```
label = 1  if close[T+1] > close[T] * (1 + threshold)   # UP by > threshold %
label = 0  if close[T+1] < close[T] * (1 - threshold)   # DOWN by > threshold %
# threshold typically 0.5–1.0% for daily bars
# discard ambiguous middle-zone samples
```

**Feature candidates (all derivable from existing Parquet data):**

| Category | Features |
|---|---|
| Price | log-return(1d), log-return(5d), log-return(20d) |
| Volume | volume ratio vs 20d avg, on-balance volume delta |
| Momentum | RSI(14), RSI(5), MACD histogram, MACD signal |
| Trend | distance from MA(50), distance from MA(200), MA(50)/MA(200) ratio |
| Volatility | Bollinger band width, ATR(14), realised vol(20d) |
| Seasonality | day-of-week, month, days-until/since earnings (if available) |
| Cross-asset | BTC return (for stock: crypto fear index as macro signal) |

**Model options:**
- **XGBoost / LightGBM** — strong baseline for tabular financial data; handles non-linear interactions well; fast to train and retrain daily.
- **Random Forest** — more robust to overfitting than gradient boosting on small datasets; lower accuracy ceiling.
- **Logistic Regression** — interpretable; useful as a sanity-check baseline to confirm that features carry signal.

**Training / retraining cadence:**
- Full retrain: monthly (or after a regime shift, e.g. drawdown > 20% detected).
- Walk-forward validation: train on rolling 2-year window, validate on 3-month out-of-sample.
- Never use future data in features (strict temporal split).

---

#### 5.2.2 Regression models (predict return magnitude)

**Task:** predict the N-day forward return directly (not just direction).

**Why it matters:** direction prediction alone does not tell you how large to size the
position. A regression output (predicted return) can feed directly into a Kelly-fraction
position sizer: `size = f(predicted_return, predicted_volatility)`.

**Model options:**
- **Ridge / Lasso regression** — useful baseline; Lasso performs implicit feature selection.
- **LightGBM Regressor** — handles non-linearity; same feature set as classification.
- **Quantile regression** — predicts a distribution of returns rather than a point estimate; enables better risk-adjusted sizing.

**Caveat:** financial returns are notoriously noisy; regression R² is typically very low
(< 0.05 in-sample on daily data). Use predicted return as one input to position sizing,
not as a ground truth.

---

#### 5.2.3 Reinforcement learning (end-to-end trading policy)

**Task:** train an agent to directly optimise a reward signal (e.g. Sharpe ratio, total
return, drawdown-adjusted return) by taking actions (buy / sell / hold) in a simulated
market environment.

**Framework options:**
- **Stable-Baselines3 + a custom Gym environment** wrapping the historical OHLCV data.
- **FinRL** — purpose-built financial RL library with built-in Gym envs for stocks/crypto.

**Why consider it:**
- Directly optimises the objective you care about (P&L, Sharpe) rather than an indirect
  proxy (classification accuracy).
- Can learn complex multi-step strategies (e.g. scale in over multiple days) that
  rule-based or single-step classifiers cannot represent.

**Why it is high-risk for a first version:**
- Requires a well-validated backtesting environment; bugs in the simulation lead to
  *reward hacking* (the agent exploits simulation artefacts, not real market patterns).
- Extremely sensitive to hyperparameters.
- Needs significantly more historical data per asset than supervised approaches.
- Interpreting the agent's policy is difficult; debugging failures in live trading is hard.

**Recommendation:** defer RL to a later phase. Build supervised ML first, validate live,
then explore RL as an enhancement.

---

### 5.3 Backtesting framework

All ML approaches require a rigorous backtesting pipeline before any live deployment.
Without it, out-of-sample performance is unknown and overfitting is invisible.

**Required components:**

```
Historical OHLCV (MinIO)
         │
         ▼
Feature Engineering (same Spark job used in production)
         │
         ▼
Walk-Forward Backtester
  ├── fold 1: train [T0..T1], test [T1..T2]
  ├── fold 2: train [T0..T2], test [T2..T3]
  └── fold N: train [T0..TN], test [TN..TN+1]
         │
         ▼
Performance Report
  ├── Total return, annualised return
  ├── Sharpe ratio, Sortino ratio
  ├── Max drawdown, max drawdown duration
  ├── Win rate, profit factor
  ├── Long/short breakdown
  └── Per-symbol breakdown
```

**Walk-forward is non-negotiable.** A single train/test split on financial data produces
optimistic performance estimates because of distribution shift over time. Walk-forward
folds give a realistic picture of how the model generalises to unseen future data.

**Transaction costs must be included.** Apply realistic slippage (0.05–0.1% per trade for
liquid crypto, 0.1–0.3% for Vietnamese equities including stamp duty) and exchange fees in
the backtester. Strategies that look profitable before costs frequently are not after.

### 5.4 Model registry and inference pipeline

```
Training job (offline, monthly)
    └── writes model artefacts to MinIO model-registry/
            e.g. model-registry/xgb_btc_v3/model.pkl
                 model-registry/xgb_btc_v3/feature_schema.json
                 model-registry/xgb_btc_v3/metadata.json

Inference job (Dagster asset, daily after indicator computation)
    ├── reads latest model version from model-registry/
    ├── reads latest features from market-analysis/
    ├── applies model → predicted direction + confidence
    └── writes signals to market-signals/ (same schema as Options A/B)
```

`metadata.json` per model version:
```json
{
  "model_id": "xgb_btc_v3",
  "trained_at": "2026-05-25T00:00:00Z",
  "train_period": ["2023-01-01", "2026-02-28"],
  "validation_sharpe": 1.42,
  "validation_max_drawdown": -0.12,
  "feature_schema_version": "2",
  "promoted": true
}
```

Only models with `"promoted": true` are loaded by the inference job. Promotion is a manual
step after reviewing the walk-forward validation report — never automated.

### 5.5 New repos and services

| Component | Repo | Notes |
|---|---|---|
| Feature engineering | `mekong-jobs` | Extend existing Spark jobs |
| Model training | `mekong-ml` (new) | Jupyter + scripts for offline training |
| Walk-forward backtester | `mekong-ml` | Python, reads MinIO Parquet |
| Model registry | MinIO `model-registry` bucket | Artefacts + metadata |
| Inference Spark job | `mekong-jobs` | Reads model → writes signals |
| Inference Dagster asset | `mekong-dagster` | Downstream of `technical_indicators` |
| Executor | `mekong-trader` | Same as Option A |

### 5.6 Pros

- **Can capture non-linear patterns** that rule-based strategies miss (e.g. multi-indicator interactions, regime-dependent signals).
- **Self-improving.** A retraining cadence means the model adapts to changing market conditions over time.
- **Confidence scores.** A classification probability can drive variable position sizing (scale in when confidence is high).
- **Feature importance.** Tree-based models (XGBoost, LightGBM) provide feature importances, giving insight into what the market is doing.
- **Composable.** A well-validated ML signal can be combined with rule-based signals as part of an ensemble.

### 5.7 Cons

- **Highest complexity** of all three options. Requires expertise in ML, feature engineering, and financial backtesting — not just software engineering.
- **Overfitting risk is severe.** Financial time series have low signal-to-noise. Without rigorous walk-forward validation, apparent out-of-sample performance is often overfitting.
- **Regime shift fragility.** A model trained on 2023–2026 data may perform poorly after a structural market change (interest rate regime shift, exchange policy change, etc.).
- **Data scarcity for Vietnamese equities.** vnstock history may be limited or inconsistent for some smaller-cap stocks; ML models need substantial clean history.
- **Operational burden.** Model registry, versioning, promotion workflow, drift detection, and retraining pipelines all require maintenance.
- **Black box.** When a model generates a bad trade, it can be difficult to understand why and to decide whether to retrain, roll back, or halt.
- **Slowest to production.** Building a trustworthy ML pipeline takes weeks to months; rule-based strategies can be live in days.

---

## 6. Cross-Cutting Concerns

These apply regardless of which option is chosen.

### 6.1 Risk management (non-negotiable)

The risk engine must enforce hard stops that cannot be overridden by signals.

| Guardrail | Typical value | Description |
|---|---|---|
| `MAX_DAILY_LOSS_PCT` | 2% | Halt all new buys if portfolio is down > N% today |
| `MAX_DRAWDOWN_PCT` | 10% | Halt all trading if NAV falls > N% from peak |
| `MAX_OPEN_POSITIONS` | 5 | Never hold more than N positions simultaneously |
| `MAX_ASSET_PCT` | 20% | No single asset > N% of portfolio value |
| `MAX_ORDER_SIZE_USD` | 500 | Hard cap on any single order |
| `ENABLED_VENUES` | `["binance"]` | Whitelist of allowed execution venues |
| `ENABLED_ASSET_CLASSES` | `["crypto"]` | Whitelist during Phase 1 |

### 6.2 Paper trading mode

The executor must support `DRY_RUN=true` (default). In this mode:
- All risk checks run normally.
- Orders are logged as if they were submitted.
- Fill prices are simulated using the price at signal time plus a configurable slippage factor.
- Position tracking and P&L computation run against simulated fills.
- All audit records are written to MinIO under a `paper/` prefix.

Paper trading should run for a minimum of 4 weeks and pass the following gates before live mode is enabled:
- No risk guardrail triggered unexpectedly.
- Simulated Sharpe > 0.5 over the paper period.
- No duplicate order submissions observed in audit log.
- Executor restart recovery confirmed (positions correctly bootstrapped from audit log).

### 6.3 Order management and idempotency

Each signal carries a `signal_id` (UUID). The order manager maintains a durable set of
`executed_signal_ids` in MinIO. Before submitting any order:

1. Check if `signal_id` is already in `executed_signal_ids`.
2. If yes: skip (already acted on).
3. If no: write `signal_id` to `executed_signal_ids` (pre-commit), then submit.

The pre-commit write means that even if the executor crashes after writing but before the
exchange confirms, the signal will not be resubmitted on restart. This trades a small risk
of a missed order for the guarantee of never doubling up.

### 6.4 Audit trail

Every event is written to MinIO `trade-audit` as a Parquet file partitioned by date:

```
trade-audit/
  signals/year=2026/month=05/day=25/signals.parquet
  orders/year=2026/month=05/day=25/orders.parquet
  fills/year=2026/month=05/day=25/fills.parquet
  positions/year=2026/month=05/day=25/snapshot.parquet
  risk_events/year=2026/month=05/day=25/rejections.parquet
```

This enables post-hoc analysis of strategy performance, risk event frequency, and fill
quality (slippage vs mid-price).

### 6.5 Monitoring and alerting

Extend the existing Telegram alert infrastructure:

| Alert | Trigger |
|---|---|
| Order submitted | Every live order (not paper) |
| Order failed | Exchange rejection or timeout after 3 retries |
| Risk guardrail triggered | Any hard stop activated |
| Daily P&L summary | 18:00 ICT daily — realised P&L, open positions, NAV |
| Drawdown warning | Portfolio NAV falls > 5% from peak (before the 10% halt) |
| Executor restart | Service comes back up (confirms recovery) |

A new `trading_health_sensor` in Dagster monitors the `trade-audit` bucket for stale
activity (no fills or signals in the last 24 h during a trading day) and alerts via
Telegram.

---

## 7. Recommended Phased Approach

### Phase 1 — Rule-based batch signals + paper trading (Option A)

**Duration:** 2–4 weeks

1. Add `market-signals` and `trade-audit` MinIO buckets.
2. Implement 2–3 rule-based strategies in a new `trade_signals` Spark job.
3. Add `trade_signals` as a Dagster asset downstream of `technical_indicators`.
4. Build `mekong-trader` with `DRY_RUN=true` default: SignalPoller → RiskEngine → OrderManager → CCXTExecutor (paper mode).
5. Run paper trading for 4 weeks; review audit trail.

**Gate to Phase 2:** Sharpe > 0.5, no risk engine bugs, executor restart recovery confirmed.

### Phase 2 — Live execution on crypto (Option A, live)

**Duration:** ongoing

1. Enable `DRY_RUN=false` for a small capital allocation (e.g. $200 USDT).
2. Monitor daily P&L summary alerts; review audit trail weekly.
3. Tune risk guardrails based on observed behaviour.

### Phase 3 — Add real-time signals for crypto (Option B, additive)

**Duration:** 3–5 weeks

1. Implement Flink signal job for short-window indicators (RSI(14), MACD).
2. Create `trade.signal` Kafka topic.
3. Add `KafkaSignalConsumer` to `mekong-trader` alongside the existing `SignalPoller`.
4. Paper trade the real-time signals in parallel with the batch signals for 2 weeks.

**Gate to Phase 4:** real-time signal quality comparable to batch signals on same period.

### Phase 4 — ML signals (Option C, additive)

**Duration:** 6–10 weeks

1. Build walk-forward backtesting framework in `mekong-ml`.
2. Train and validate XGBoost classifier on existing OHLCV + indicator features.
3. Add inference Dagster asset; paper trade ML signals.
4. Promote to live only after walk-forward Sharpe > 1.0 and 4-week paper trading gate.

### Phase 5 — Vietnamese equities execution (all options)

**Duration:** depends on broker API quality

1. Evaluate SSI FastConnect or VPS API; implement broker adapter in `mekong-trader`.
2. Handle T+2 settlement, trading session constraints, lot sizes.
3. Paper trade (signals are already generated; only execution is new).

---

## 8. New Repository: `mekong-trader`

### 8.1 Directory layout

```
mekong-trader/
  main.py                      # entry: python main.py trader
  config.py                    # loads all ENV vars, validates required ones
  signal_poller.py             # MinIO-based signal reader (Option A)
  kafka_signal_consumer.py     # Kafka-based signal reader (Option B)
  risk_engine.py               # guardrails, daily loss tracking
  order_manager.py             # sizing, dedup, pre-commit write
  executors/
    base.py                    # abstract Executor interface
    ccxt_executor.py           # Binance via CCXT
    paper_executor.py          # DRY_RUN mode
  position_tracker.py          # in-memory book, MinIO bootstrap
  audit_writer.py              # writes Parquet to trade-audit
  alerts.py                    # Telegram notifications
  requirements.txt
  Dockerfile                   # extends python:3.12-slim
```

### 8.2 Key environment variables

| Variable | Default | Description |
|---|---|---|
| `DRY_RUN` | `true` | Paper trading mode |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | For Option B |
| `MINIO_ENDPOINT` | `http://minio:9000` | |
| `CCXT_EXCHANGE` | `binance` | Exchange ID |
| `CCXT_API_KEY` | — | Must be set for live mode |
| `CCXT_API_SECRET` | — | Must be set for live mode |
| `MAX_DAILY_LOSS_PCT` | `2.0` | |
| `MAX_DRAWDOWN_PCT` | `10.0` | |
| `MAX_OPEN_POSITIONS` | `5` | |
| `MAX_ASSET_PCT` | `20.0` | |
| `MAX_ORDER_SIZE_USD` | `500.0` | |
| `RISK_PER_TRADE_PCT` | `1.0` | Fixed-fraction Kelly input |
| `SIGNAL_POLL_INTERVAL_S` | `60` | MinIO poll cadence |
| `ENABLED_VENUES` | `binance` | Comma-separated |
| `TELEGRAM_BOT_TOKEN` | — | For alerts |
| `TELEGRAM_CHAT_ID` | — | For alerts |

### 8.3 Docker Compose service (to be added to `mekong-infra`)

```yaml
  mekong-trader:
    build:
      context: ${MEKONG_TRADER_DIR:-../mekong-trader}
      dockerfile: Dockerfile
    container_name: mekong-trader
    restart: unless-stopped
    command: python main.py trader
    environment:
      DRY_RUN: ${DRY_RUN:-true}
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY:-minioadmin}
      MINIO_SECRET_KEY: ${MINIO_SECRET_KEY:-minioadmin}
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      CCXT_EXCHANGE: ${CCXT_EXCHANGE:-binance}
      CCXT_API_KEY: ${CCXT_API_KEY:-}
      CCXT_API_SECRET: ${CCXT_API_SECRET:-}
      MAX_DAILY_LOSS_PCT: ${MAX_DAILY_LOSS_PCT:-2.0}
      MAX_DRAWDOWN_PCT: ${MAX_DRAWDOWN_PCT:-10.0}
      MAX_OPEN_POSITIONS: ${MAX_OPEN_POSITIONS:-5}
      MAX_ASSET_PCT: ${MAX_ASSET_PCT:-20.0}
      MAX_ORDER_SIZE_USD: ${MAX_ORDER_SIZE_USD:-500.0}
      RISK_PER_TRADE_PCT: ${RISK_PER_TRADE_PCT:-1.0}
      TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN:-}
      TELEGRAM_CHAT_ID: ${TELEGRAM_CHAT_ID:-}
    depends_on:
      minio:
        condition: service_healthy
      kafka:
        condition: service_healthy
    deploy:
      resources:
        limits:
          memory: 512m
        reservations:
          memory: 128m
```

---

## 9. Option Comparison Summary

| Criterion | Option A (Batch rules) | Option B (Flink stream) | Option C (ML) |
|---|---|---|---|
| Signal latency | Daily (next-day) | Seconds | Daily (next-day) |
| Implementation effort | Low | High | Very high |
| Backtesting support | Easy | Hard | Requires framework |
| Operational complexity | Low | High | High |
| Overfitting risk | Low | Low | High |
| Intra-day capture | No | Yes | No (daily model) |
| Adapts to market change | No (static rules) | No (static logic) | Yes (retraining) |
| Interpretability | High | High | Medium (trees) |
| Recommended phase | 1 (start here) | 3 (additive) | 4 (additive) |

---

## 10. Open Questions

1. **Capital allocation.** What is the initial live capital budget? This determines whether fixed-fraction sizing or simpler fixed-lot sizing is appropriate.
2. **Benchmark.** What does "good" performance look like? BTC buy-and-hold? VN-Index? Define before deploying live.
3. **Tax / regulatory.** Are there reporting obligations for algorithmic trading on Vietnamese equities? Crypto tax treatment in Vietnam?
4. **Multiple exchanges.** Is cross-exchange arbitrage (e.g. Binance vs Bybit price difference) in scope? If yes, the CCXT executor needs to manage balances across multiple accounts.
5. **Short selling.** Is short selling in scope? Binance supports it via margin/futures. Vietnamese equities have no short-selling mechanism for retail.
6. **Universe size.** How many symbols should the bot trade simultaneously? Larger universes increase diversification but also operational complexity and the risk of correlated drawdowns.
