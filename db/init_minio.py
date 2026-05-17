"""Create MinIO buckets and apply lifecycle rules. Safe to re-run — idempotent.

  market-data     — raw price snapshots; 30-day object expiry
  market-analysis — OHLCV Parquet bars; no expiry (kept indefinitely)
"""
import os

from dotenv import load_dotenv
from minio import Minio
from minio.commonconfig import ENABLED
from minio.lifecycleconfig import Expiration, Filter, LifecycleConfig, Rule

load_dotenv()

RETENTION_DAYS = 30


def _client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    secure   = endpoint.startswith("https://")
    host     = endpoint.split("://", 1)[-1]
    return Minio(
        host,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=secure,
    )


def _ensure(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Created bucket '{bucket}'.")
    else:
        print(f"Bucket '{bucket}' already exists.")


def _set_expiry(client: Minio, bucket: str, days: int) -> None:
    config = LifecycleConfig(
        [Rule(ENABLED, rule_filter=Filter(prefix=""), rule_id="expire-all",
              expiration=Expiration(days=days))]
    )
    client.set_bucket_lifecycle(bucket, config)


def run() -> None:
    client = _client()

    raw = os.getenv("MINIO_BUCKET", "market-data")
    _ensure(client, raw)
    _set_expiry(client, raw, RETENTION_DAYS)
    print(f"Bucket '{raw}' ready — objects expire after {RETENTION_DAYS} days.")

    analysis = os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis")
    _ensure(client, analysis)
    print(f"Bucket '{analysis}' ready — no expiry (OHLCV bars kept indefinitely).")


if __name__ == "__main__":
    run()
