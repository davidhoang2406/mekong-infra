# TEST.md

Testing strategy for the Vietnamese Stock Streaming Pipeline.

## Tiers

Tests are split by external dependency:

| Tier | Marker | Requires | Speed |
|---|---|---|---|
| **Unit** | `@pytest.mark.unit` | Nothing (pure Python) | < 1 s total |
| **Integration** | `@pytest.mark.integration` | Docker Compose up, Kafka + MinIO healthy | Seconds–minutes |

## Running tests

```bash
# Unit only (no Docker needed)
.venv/bin/pytest -m unit

# Integration only (Docker must be running)
.venv/bin/pytest -m integration

# Everything
.venv/bin/pytest
```

## File layout

```
tests/
├── conftest.py                  # shared fixtures (MinIO s3_client, Kafka producer/consumer)
├── unit/
│   ├── test_message.py          # build_envelope
│   ├── test_coerce.py           # _coerce_float / _coerce_int / _to_ts
│   ├── test_alert_rules.py      # _check() — the core alert logic
│   └── test_storage_routes.py   # _EXTRACTORS in storage_consumer
└── integration/
    ├── test_price_pipeline.py   # produce → Kafka → _Buffer → MinIO → verify
    ├── test_ohlcv_pipeline.py   # ohlcv bar round-trip
    └── test_alert_pipeline.py   # threshold triggers end-to-end
```

## `pytest.ini`

```ini
[pytest]
markers =
    unit: no external dependencies
    integration: requires Docker Compose (Kafka + MinIO)
```

---

## Unit test cases

### `tests/unit/test_message.py` — envelope schema

| Test | What it checks |
|---|---|
| `test_all_fields_present` | returned dict has all 6 keys |
| `test_default_timestamp_is_utc` | timestamp ends with `+00:00` when no override |
| `test_custom_timestamp_passes_through` | explicit timestamp is not replaced |
| `test_payload_is_not_mutated` | original payload dict is untouched |

```python
from schemas.message import build_envelope

def test_all_fields_present():
    msg = build_envelope("price.snapshot", "VCB", "HOSE", {"price": 85000})
    assert {"event_type", "symbol", "exchange", "timestamp", "source", "payload"} == set(msg)

def test_custom_timestamp_passes_through():
    msg = build_envelope("ohlcv.bar", "VCB", "HOSE", {}, timestamp="2024-05-10T00:00:00+00:00")
    assert msg["timestamp"] == "2024-05-10T00:00:00+00:00"
```

---

### `tests/unit/test_coerce.py` — defensive type coercion

| Test | Input | Expected |
|---|---|---|
| `test_float_none` | `None` | `0.0` |
| `test_float_bad_string` | `"N/A"` | `0.0` |
| `test_float_valid` | `"85000.5"` | `85000.5` |
| `test_int_none` | `None` | `0` |
| `test_int_float_string` | `"1234.9"` | `1234` |
| `test_to_ts_bare_date` | `"2024-05-10"` | `"2024-05-10T00:00:00+00:00"` |
| `test_to_ts_pandas_timestamp` | `pd.Timestamp("2024-05-10")` | contains `"2024-05-10"` |

---

### `tests/unit/test_alert_rules.py` — rule evaluation

`evaluate_rules()` (in `producers/utils.py`) is pure logic with no I/O — used by `PriceAlertJob` (Flink) and exercised directly in unit tests.

| Test | Rule | Payload | Expected |
|---|---|---|---|
| `test_wildcard_sharp_drop_fires` | `* pct_change <= -3.0` | `pct_change=-3.5` | 1 hit |
| `test_wildcard_no_trigger_below_threshold` | `* pct_change <= -3.0` | `pct_change=-1.0` | `[]` |
| `test_symbol_specific_match_fires` | `VCB pct_change >= 2.0` | symbol=VCB, pct=2.5 | 1 hit |
| `test_symbol_specific_skips_other_symbol` | `VCB pct_change >= 2.0` | symbol=ACB, pct=2.5 | `[]` |
| `test_zero_price_flag_fires` | `* price == 0.0` | `price=0.0` | 1 hit |
| `test_operator` (parametrized) | one rule per operator | boundary values | correct fire/skip |

```python
from producers.utils import evaluate_rules

RULES = [{"symbol": "*", "field": "pct_change", "operator": "<=", "threshold": -3.0, "message": "drop"}]

def test_wildcard_sharp_drop_fires():
    hits = evaluate_rules(RULES, "HPG", {"price": 50.0, "pct_change": -3.5})
    assert len(hits) == 1

def test_wildcard_no_trigger_below_threshold():
    assert evaluate_rules(RULES, "HPG", {"price": 50.0, "pct_change": -1.0}) == []
```

---

### `tests/unit/test_storage_routes.py` — row extractor lambdas

Each `_EXTRACTORS` extractor is a pure function: message dict → row dict. Verify field names and values.

| Test | Event type | Checks |
|---|---|---|
| `test_price_snapshot_extractor` | `price.snapshot` | `symbol` and `price` fields present with correct values |
| `test_ohlcv_bar_extractor` | `ohlcv.bar` | `open`, `close` fields present |
| `test_financials_report_extractor` | `financials.report` | `report_date` field present |
| `test_unknown_event_type_ignored` | `"unknown"` | `buf.add()` does not write any row |

---

## Integration test cases

All integration tests use a shared `conftest.py` fixture that:
1. Verifies Kafka and MinIO are reachable (skips with `pytest.skip` if not)
2. Creates a boto3 `s3_client` pointed at the local MinIO instance
3. Cleans up all test objects (keyed with `symbol=__TEST__`) after each test

### `tests/integration/test_price_pipeline.py`

| Test | Steps | Assert |
|---|---|---|
| `test_price_snapshot_written_to_minio` | Build a `price.snapshot` msg → `_Buffer.add()` → `_Buffer.flush()` | Object exists in MinIO under `price.snapshot/symbol=__TEST__/` |
| `test_avro_partition_path_structure` | Same flush | Key matches `price.snapshot/symbol=.../year=.../month=.../day=.../part-*.avro` |
| `test_extractor_produces_correct_fields` | Call `_EXTRACTORS["price.snapshot"]` directly | Row dict has `symbol`, `price`, `pct_change` |
| `test_consumer_group_isolation` | Produce 1 msg → two consumers in different groups each read it | Both consumers receive the message |

### `tests/integration/test_ohlcv_pipeline.py`

| Test | Steps | Assert |
|---|---|---|
| `test_ohlcv_bar_uses_trading_date_not_insertion_time` | Build `ohlcv.bar` with a past trading date → flush | Object key contains the trading date, not today's date |
| `test_ohlcv_avro_schema` | Flush one bar → read back with fastavro | Record has `open`, `high`, `low`, `close`, `volume` fields |
| `test_ohlcv_extractor_fields` | Call `_EXTRACTORS["ohlcv.bar"]` directly | Row dict has all OHLCV fields |

### Alert path — covered by unit tests only

The earlier `tests/integration/test_alert_pipeline.py` exercised `consumers/alert_consumer.py` via a real Kafka round-trip. With `alert_consumer` removed, integration coverage for alerts would need a Flink-cluster harness (out of scope for the unit/integration split in this repo). Rule correctness is covered by `tests/unit/test_alert_rules.py` against the shared `evaluate_rules()` function.
