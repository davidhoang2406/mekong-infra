FROM python:3.12-slim

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Pre-install dependencies (mirrors mekong-kafka/requirements.txt).
# mekong-kafka source is live-mounted at runtime via ${MEKONG_KAFKA_DIR}.
RUN pip install --no-cache-dir \
    "kafka-python==2.3.1" \
    "fastavro>=1.9" \
    "pyarrow>=16.0" \
    "minio>=7.2" \
    "python-dotenv==1.2.2" \
    "requests==2.34.0" \
    "vnstock==4.0.2" \
    "ccxt>=4.4.0" \
    "market-data-models @ git+https://github.com/davidhoang2406/mekong-data-models.git"

RUN useradd -m -u 1000 mekong && mkdir -p /opt/project
WORKDIR /opt/project
ENV PYTHONPATH=/opt/project
USER mekong
