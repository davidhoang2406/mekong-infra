FROM python:3.12-slim

RUN pip install --no-cache-dir \
    "dagster==1.13.5" \
    "dagster-webserver==1.13.5" \
    "docker>=7.0" \
    "minio>=7.2" \
    "python-dotenv>=1.0" \
    "pyarrow>=14.0" \
    "s3fs>=2024.2" \
    "kafka-python==2.3.1"

RUN mkdir -p /opt/dagster/dagster_home/storage

# Instance config — baked in; named volume dagster_storage mounts only the
# storage/ subdirectory so it never overwrites this file.
COPY dagster/dagster.yaml /opt/dagster/dagster_home/dagster.yaml

ENV DAGSTER_HOME=/opt/dagster/dagster_home
# /opt/project  → mekong-jobs source (mounted at runtime)
# /opt/dagster/app → dagster_project package (mounted at runtime)
ENV PYTHONPATH=/opt/project:/opt/dagster/app
