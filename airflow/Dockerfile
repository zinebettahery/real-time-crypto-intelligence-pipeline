# ================================================================
# Custom Airflow image for real-time-crypto-intelligence-pipeline
# Base : apache/airflow:2.8.1-python3.11
# Adds : confluent-kafka, websocket-client, requests, pandas
#        (same libs already used in news_producer & trades_producer)
# ================================================================
FROM apache/airflow:2.8.1-python3.11

USER root

# librdkafka — required by confluent-kafka C extension
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc \
       librdkafka-dev \
       curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt