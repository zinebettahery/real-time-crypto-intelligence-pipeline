"""
INVISTIS — DAG: fred_daily_ingestion
Fetches FRED macro indicators daily → validates → saves Parquet → publishes to fred_topic.

Wired to .env credentials:
  KAFKA_BOOTSTRAP_SERVERS = pkc-921jm.us-east-2.aws.confluent.cloud:9092
  KAFKA_API_KEY            = C24NDWF2AUGFCN6G
  FRED_API_KEY             = 704fdac2c75a836581cb74008d766882
  NEWSAPI_KEY              = b61e2b4006ce493599bb179be55cfc89
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ── FRED series ───────────────────────────────────────────────────────────────
FRED_SERIES = {
    "DFF":      "Federal Funds Rate",
    "CPIAUCSL": "Consumer Price Index",
    "UNRATE":   "Unemployment Rate",
    "GDP":      "Gross Domestic Product",
    "PCE":      "Personal Consumption Expenditures",
    "M2SL":     "M2 Money Stock",
    "PAYEMS":   "All Employees Total Nonfarm",
    "INDPRO":   "Industrial Production Index",
    "FEDFUNDS": "Effective Federal Funds Rate",
    "TB3MS":    "3-Month Treasury Bill Rate",
}

# Output path inside Docker container (mounted from ./airflow/data on host)
DATA_PATH = Path("/opt/airflow/data/fred")


def _get_var(name: str) -> str:
    """
    Get Airflow Variable and strip surrounding quotes.
    Docker Compose sometimes preserves the quotes from .env values
    e.g. "pkc-921jm..." becomes the literal string including the quote chars.
    """
    value = Variable.get(name)
    return value.strip('"').strip("'")


# ── DAG ───────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "invistis",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fred_daily_ingestion",
    description="FRED macro → Parquet + fred_topic (Confluent pkc-921jm)",
    schedule_interval="0 7 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["invistis", "fred", "macro", "batch"],
) as dag:

    # ── Task 1: fetch ─────────────────────────────────────────────────────────
    def fetch_fred_data(**context):
        fred_api_key   = _get_var("FRED_API_KEY")
        execution_date = context["ds"]
        start_date     = (
            datetime.strptime(execution_date, "%Y-%m-%d") - timedelta(days=7)
        ).strftime("%Y-%m-%d")

        all_records = []

        for series_id, series_name in FRED_SERIES.items():
            try:
                resp = requests.get(
                    "https://api.stlouisfed.org/fred/series/observations",
                    params={
                        "series_id":         series_id,
                        "api_key":           fred_api_key,
                        "file_type":         "json",
                        "observation_start": start_date,
                        "sort_order":        "desc",
                        "limit":             5,
                    },
                    timeout=30,
                )
                resp.raise_for_status()

                added = 0
                for obs in resp.json().get("observations", []):
                    if obs["value"] == ".":
                        continue
                    all_records.append({
                        "series_id":   series_id,
                        "series_name": series_name,
                        "date":        obs["date"],
                        "value":       float(obs["value"]),
                        "fetched_at":  execution_date,
                    })
                    added += 1

                log.info("✅ %s → %d records", series_id, added)

            except requests.HTTPError as e:
                log.error("❌ HTTP %s: %s", series_id, e)
            except Exception as e:
                log.error("❌ Error %s: %s", series_id, e)

        if not all_records:
            raise ValueError("No data fetched from FRED — check FRED_API_KEY.")

        context["ti"].xcom_push(key="fred_records", value=all_records)
        log.info("Total records: %d", len(all_records))
        return len(all_records)

    # ── Task 2: validate + store ──────────────────────────────────────────────
    def validate_and_store(**context):
        records = context["ti"].xcom_pull(key="fred_records", task_ids="fetch_fred_data")
        if not records:
            raise ValueError("No records from fetch_fred_data.")

        df = pd.DataFrame(records)

        # Schema checks
        missing_cols = {"series_id", "series_name", "date", "value", "fetched_at"} - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing columns: {missing_cols}")
        if df["value"].isna().sum():
            raise ValueError("Null values found in 'value' column")
        unknown = set(df["series_id"]) - set(FRED_SERIES)
        if unknown:
            raise ValueError(f"Unknown series: {unknown}")

        log.info("✅ Validation passed — %d rows, %d series", len(df), df["series_id"].nunique())
        log.info("\n%s", df.groupby("series_id")["value"].last().to_string())

        # Save files
        execution_date = context["ds"]
        out_dir = DATA_PATH / f"date={execution_date}"
        out_dir.mkdir(parents=True, exist_ok=True)

        df.to_parquet(out_dir / "fred_indicators.parquet", index=False)
        df.to_csv(out_dir / "fred_indicators.csv", index=False)
        log.info("✅ Saved to %s", out_dir)

        context["ti"].xcom_push(key="validated_records", value=records)

    # ── Task 3: publish to Kafka ──────────────────────────────────────────────
    def publish_to_kafka(**context):
        records = context["ti"].xcom_pull(key="validated_records", task_ids="validate_and_store")
        if not records:
            log.warning("No records — skipping Kafka publish.")
            return

        # Read credentials — same names as kafka_config.py
        try:
            bootstrap  = _get_var("KAFKA_BOOTSTRAP_SERVERS")
            api_key    = _get_var("KAFKA_API_KEY")
            api_secret = _get_var("KAFKA_API_SECRET")
        except KeyError as e:
            log.error("❌ Missing Variable: %s — skipping.", e)
            return

        try:
            from confluent_kafka import Producer, KafkaException

            # Same Producer config block as news_producer.py / trades_producer.py
            producer = Producer({
                "bootstrap.servers": bootstrap,
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms":   "PLAIN",
                "sasl.username":     api_key,
                "sasl.password":     api_secret,
            })

            published = 0
            errors    = 0

            for record in records:
                try:
                    producer.produce(
                        "fred_topic",
                        key=record["series_id"],
                        value=json.dumps(record).encode("utf-8"),
                    )
                    published += 1
                except KafkaException as e:
                    log.error("Produce error %s: %s", record["series_id"], e)
                    errors += 1

            producer.flush(timeout=30)
            log.info("✅ Kafka: %d/%d published (%d errors)", published, len(records), errors)

        except ImportError:
            log.error("confluent-kafka not installed.")
        except Exception as e:
            # Non-fatal — Parquet is already saved
            log.error("❌ Kafka publish failed (non-fatal): %s", e)

    # ── Wire up ───────────────────────────────────────────────────────────────
    t1 = PythonOperator(task_id="fetch_fred_data",    python_callable=fetch_fred_data)
    t2 = PythonOperator(task_id="validate_and_store", python_callable=validate_and_store)
    t3 = PythonOperator(task_id="publish_to_kafka",   python_callable=publish_to_kafka)

    t1 >> t2 >> t3