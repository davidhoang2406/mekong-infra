"""Delete all objects from a MinIO bucket (irreversible).

Usage: python db/flush_minio.py <bucket>
Called by `make storage-flush` after the user selects which buckets to clear.
"""
import argparse
import os

from dotenv import load_dotenv
from minio import Minio

load_dotenv()


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


def run(bucket: str) -> None:
    client  = _client()
    objects = list(client.list_objects(bucket, recursive=True))
    if not objects:
        print(f"Bucket '{bucket}' is already empty.")
        return
    errors = client.remove_objects(bucket, (o.object_name for o in objects))
    n = 0
    for err in errors:
        print(f"Error deleting {err.object_name}: {err}")
        n += 1
    deleted = len(objects) - n
    print(f"Deleted {deleted} object(s) from '{bucket}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "bucket",
        nargs="?",
        default=os.getenv("MINIO_BUCKET", "market-data"),
        help="Bucket to flush (default: $MINIO_BUCKET or market-data)",
    )
    run(parser.parse_args().bucket)
