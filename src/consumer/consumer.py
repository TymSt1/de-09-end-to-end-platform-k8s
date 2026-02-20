"""
Kafka-to-S3 Consumer
Reads taxi ride events from Kafka and writes them as Parquet files to S3 (LocalStack).
Batches events and flushes every N records or M seconds.
"""
import json
import os
import io
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Configuration
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "platform-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "taxi-rides")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "s3-sink-consumer")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localstack.data.svc.cluster.local:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "platform-raw-data")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_INTERVAL_SECONDS = int(os.getenv("FLUSH_INTERVAL_SECONDS", "30"))

# Arrow schema for taxi rides
SCHEMA = pa.schema([
    ("ride_id", pa.string()),
    ("event_timestamp", pa.string()),
    ("pickup_zone_id", pa.int32()),
    ("pickup_zone_name", pa.string()),
    ("pickup_borough", pa.string()),
    ("dropoff_zone_id", pa.int32()),
    ("dropoff_zone_name", pa.string()),
    ("dropoff_borough", pa.string()),
    ("passenger_count", pa.int32()),
    ("trip_distance_miles", pa.float64()),
    ("duration_minutes", pa.float64()),
    ("payment_type", pa.string()),
    ("rate_code", pa.string()),
    ("fare_amount", pa.float64()),
    ("tip_amount", pa.float64()),
    ("tolls_amount", pa.float64()),
    ("total_amount", pa.float64()),
])


def create_s3_client():
    """Create boto3 S3 client pointing at LocalStack."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


def flush_to_s3(s3_client, records):
    """Write a batch of records to S3 as a Parquet file."""
    if not records:
        return

    # Build columnar data from records
    columns = {field.name: [] for field in SCHEMA}
    for record in records:
        for field in SCHEMA:
            columns[field.name].append(record.get(field.name))

    table = pa.table(columns, schema=SCHEMA)

    # Write to in-memory buffer
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    # Upload to S3 with date-based partitioning
    now = datetime.utcnow()
    key = f"taxi-rides/year={now.year}/month={now.month:02d}/day={now.day:02d}/{now.strftime('%H%M%S')}_{len(records)}.parquet"

    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    logger.info(f"Flushed {len(records)} records to s3://{S3_BUCKET}/{key}")


def main():
    """Main consumer loop."""
    logger.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}")
    logger.info(f"Consuming from topic: {TOPIC}")
    logger.info(f"Writing to S3: {S3_ENDPOINT}/{S3_BUCKET}")
    logger.info(f"Batch size: {BATCH_SIZE}, flush interval: {FLUSH_INTERVAL_SECONDS}s")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=5000,
    )

    s3_client = create_s3_client()
    batch = []
    last_flush = time.time()
    total_flushed = 0

    try:
        while True:
            # Poll for messages
            messages = consumer.poll(timeout_ms=1000)
            for tp, records in messages.items():
                for record in records:
                    batch.append(record.value)

            # Flush if batch is full or time interval passed
            elapsed = time.time() - last_flush
            if len(batch) >= BATCH_SIZE or (batch and elapsed >= FLUSH_INTERVAL_SECONDS):
                flush_to_s3(s3_client, batch)
                total_flushed += len(batch)
                batch = []
                last_flush = time.time()
                logger.info(f"Total records flushed to S3: {total_flushed}")

    except KeyboardInterrupt:
        logger.info("Shutting down consumer")
    finally:
        if batch:
            flush_to_s3(s3_client, batch)
            total_flushed += len(batch)
        consumer.close()
        logger.info(f"Consumer stopped. Total records written: {total_flushed}")


if __name__ == "__main__":
    main()
