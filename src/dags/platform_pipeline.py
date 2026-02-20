"""
Platform Pipeline DAG
Orchestrates the full data flow:
Producer -> Kafka -> Consumer -> S3 -> Spark -> PostgreSQL -> dbt
"""

from datetime import datetime, timedelta
import logging
import time
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

import subprocess
import os

logger = logging.getLogger(__name__)

WAREHOUSE = {
    "host": os.getenv("WAREHOUSE_HOST", "postgres.data.svc.cluster.local"),
    "port": os.getenv("WAREHOUSE_PORT", "5432"),
    "dbname": os.getenv("WAREHOUSE_DB", "warehouse"),
    "user": os.getenv("WAREHOUSE_USER", "platform"),
    "password": os.getenv("WAREHOUSE_PASSWORD", "platform_secret_2026"),
}

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localstack.data.svc.cluster.local:4566")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "platform-processed-data")
DBT_PROJECT_DIR = "/opt/airflow/dbt"


default_args = {
    "owner": "platform-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def _run_kubectl(args):
    """Run a kubectl command and return output."""
    cmd = ["kubectl"] + args
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise AirflowException(f"kubectl failed: {result.stderr}")
    return result.stdout.strip()


def scale_streaming(replicas, **kwargs):
    """Scale producer and consumer deployments."""
    logger.info(f"Scaling streaming to {replicas} replicas")
    _run_kubectl(["scale", "deployment/taxi-producer", "-n", "streaming", f"--replicas={replicas}"])
    _run_kubectl(["scale", "deployment/taxi-consumer", "-n", "streaming", f"--replicas={replicas}"])
    logger.info(f"Streaming scaled to {replicas}")


def wait_for_data(**kwargs):
    """Wait for streaming to accumulate data."""
    wait_seconds = 60
    logger.info(f"Waiting {wait_seconds}s for data to accumulate in Kafka/S3...")
    time.sleep(wait_seconds)
    logger.info("Wait complete")


def run_spark_job(**kwargs):
    """Submit Spark batch processing job on K8s."""
    job_name = f"spark-batch-{int(time.time())}"
    logger.info(f"Submitting Spark job: {job_name}")

    _run_kubectl(["delete", "jobs", "--all", "-n", "processing", "--ignore-not-found=true"])

    job_manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name, "namespace": "processing"},
        "spec": {
            "backoffLimit": 2,
            "template": {
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "spark",
                            "image": "taxi-spark:latest",
                            "imagePullPolicy": "Never",
                            "env": [
                                {"name": "S3_ENDPOINT", "value": S3_ENDPOINT},
                                {"name": "RAW_BUCKET", "value": "platform-raw-data"},
                                {"name": "PROCESSED_BUCKET", "value": PROCESSED_BUCKET},
                            ],
                            "resources": {
                                "requests": {"memory": "1Gi", "cpu": "500m"},
                                "limits": {"memory": "2Gi", "cpu": "1000m"},
                            },
                        }
                    ],
                },
            },
        },
    }

    manifest_path = "/tmp/spark-job.json"
    with open(manifest_path, "w") as f:
        json.dump(job_manifest, f)

    _run_kubectl(["apply", "-f", manifest_path])

    logger.info("Waiting for Spark job to complete...")
    for i in range(60):
        time.sleep(5)
        output = _run_kubectl(["get", "job", job_name, "-n", "processing", "-o", "jsonpath={.status.succeeded}"])
        if output == "1":
            logger.info("Spark job completed successfully")
            return
        failed = _run_kubectl(["get", "job", job_name, "-n", "processing", "-o", "jsonpath={.status.failed}"])
        if failed and int(failed) >= 2:
            raise AirflowException("Spark job failed")

    raise AirflowException("Spark job timed out")


def load_to_staging(**kwargs):
    """Load processed Parquet data from S3 into PostgreSQL staging schema.

    Uses a temp table + INSERT ON CONFLICT pattern for rides (accumulate).
    Uses TRUNCATE + bulk INSERT for aggregation tables (replace).
    """
    import boto3
    import pyarrow.parquet as pq
    import io
    import psycopg

    logger.info("Loading processed data into PostgreSQL staging")

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    conn = psycopg.connect(**WAREHOUSE, connect_timeout=10)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    conn.commit()

    tables = {
        "cleaned-rides": {
            "table": "staging.stg_rides",
            "mode": "append",
            "unique_key": "ride_id",
            "columns": [
                "ride_id",
                "event_ts",
                "event_date",
                "event_hour",
                "pickup_zone_id",
                "pickup_zone_name",
                "pickup_borough",
                "dropoff_zone_id",
                "dropoff_zone_name",
                "dropoff_borough",
                "passenger_count",
                "trip_distance_miles",
                "duration_minutes",
                "payment_type",
                "rate_code",
                "fare_amount",
                "tip_amount",
                "tolls_amount",
                "total_amount",
                "fare_per_mile",
                "fare_per_minute",
                "tip_percentage",
                "is_rush_hour",
                "is_weekend",
            ],
            "ddl": """
                CREATE TABLE IF NOT EXISTS staging.stg_rides (
                    ride_id TEXT PRIMARY KEY,
                    event_ts TIMESTAMP,
                    event_date DATE,
                    event_hour INTEGER,
                    pickup_zone_id INTEGER,
                    pickup_zone_name TEXT,
                    pickup_borough TEXT,
                    dropoff_zone_id INTEGER,
                    dropoff_zone_name TEXT,
                    dropoff_borough TEXT,
                    passenger_count INTEGER,
                    trip_distance_miles DOUBLE PRECISION,
                    duration_minutes DOUBLE PRECISION,
                    payment_type TEXT,
                    rate_code TEXT,
                    fare_amount DOUBLE PRECISION,
                    tip_amount DOUBLE PRECISION,
                    tolls_amount DOUBLE PRECISION,
                    total_amount DOUBLE PRECISION,
                    fare_per_mile DOUBLE PRECISION,
                    fare_per_minute DOUBLE PRECISION,
                    tip_percentage DOUBLE PRECISION,
                    is_rush_hour BOOLEAN,
                    is_weekend BOOLEAN
                );
            """,
        },
        "zone-stats": {
            "table": "staging.stg_zone_stats",
            "mode": "replace",
            "columns": [
                "pickup_zone_id",
                "pickup_zone_name",
                "pickup_borough",
                "trip_count",
                "avg_total",
                "avg_tip_pct",
                "avg_distance",
                "avg_duration",
                "total_revenue",
                "avg_passengers",
                "rank_by_revenue",
            ],
            "ddl": """
                CREATE TABLE IF NOT EXISTS staging.stg_zone_stats (
                    pickup_zone_id INTEGER,
                    pickup_zone_name TEXT,
                    pickup_borough TEXT,
                    trip_count BIGINT,
                    avg_total DOUBLE PRECISION,
                    avg_tip_pct DOUBLE PRECISION,
                    avg_distance DOUBLE PRECISION,
                    avg_duration DOUBLE PRECISION,
                    total_revenue DOUBLE PRECISION,
                    avg_passengers INTEGER,
                    rank_by_revenue INTEGER
                );
            """,
        },
        "hourly-stats": {
            "table": "staging.stg_hourly_stats",
            "mode": "replace",
            "columns": [
                "event_date",
                "event_hour",
                "trip_count",
                "avg_total",
                "hourly_revenue",
                "avg_distance",
                "avg_duration",
                "total_passengers",
            ],
            "ddl": """
                CREATE TABLE IF NOT EXISTS staging.stg_hourly_stats (
                    event_date DATE,
                    event_hour INTEGER,
                    trip_count BIGINT,
                    avg_total DOUBLE PRECISION,
                    hourly_revenue DOUBLE PRECISION,
                    avg_distance DOUBLE PRECISION,
                    avg_duration DOUBLE PRECISION,
                    total_passengers BIGINT
                );
            """,
        },
        "payment-stats": {
            "table": "staging.stg_payment_stats",
            "mode": "replace",
            "columns": [
                "payment_type",
                "trip_count",
                "avg_total",
                "avg_tip_pct",
                "total_revenue",
                "pct_of_trips",
            ],
            "ddl": """
                CREATE TABLE IF NOT EXISTS staging.stg_payment_stats (
                    payment_type TEXT,
                    trip_count BIGINT,
                    avg_total DOUBLE PRECISION,
                    avg_tip_pct DOUBLE PRECISION,
                    total_revenue DOUBLE PRECISION,
                    pct_of_trips DOUBLE PRECISION
                );
            """,
        },
    }

    for s3_prefix, table_info in tables.items():
        try:
            logger.info(f"Loading {s3_prefix} into {table_info['table']} (mode={table_info['mode']})")
            cur.execute(table_info["ddl"])
            conn.commit()

            if table_info["mode"] == "replace":
                cur.execute(f"TRUNCATE TABLE {table_info['table']};")
                conn.commit()

            response = s3.list_objects_v2(Bucket=PROCESSED_BUCKET, Prefix=f"{s3_prefix}/")
            parquet_files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")]

            expected_cols = table_info["columns"]

            for parquet_key in parquet_files:
                obj = s3.get_object(Bucket=PROCESSED_BUCKET, Key=parquet_key)
                table = pq.read_table(io.BytesIO(obj["Body"].read()))
                df = table.to_pandas()

                if df.empty:
                    continue

                available_cols = [c for c in expected_cols if c in df.columns]
                df = df[available_cols]

                col_names = ", ".join(available_cols)
                placeholders = ", ".join(["%s"] * len(available_cols))

                if table_info["mode"] == "append":
                    insert_sql = f"INSERT INTO {table_info['table']} ({col_names}) VALUES ({placeholders}) ON CONFLICT ({table_info['unique_key']}) DO NOTHING"
                else:
                    insert_sql = f"INSERT INTO {table_info['table']} ({col_names}) VALUES ({placeholders})"

                batch_size = 100
                rows = []
                for _, row in df.iterrows():
                    values = tuple(None if str(v) == "nan" else v for v in row.tolist())
                    rows.append(values)

                # Batch execute for speed
                for i in range(0, len(rows), batch_size):
                    batch = rows[i : i + batch_size]
                    cur.executemany(insert_sql, batch)

                conn.commit()
                logger.info(f"Loaded {len(rows)} rows from {parquet_key}")

            count_result = cur.execute(f"SELECT COUNT(*) FROM {table_info['table']};").fetchone()
            logger.info(f"{table_info['table']}: {count_result[0]} total rows")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading {s3_prefix}: {e}")
            raise

    cur.close()
    conn.close()
    logger.info("Staging load complete")


def run_dbt(command, **kwargs):
    """Run a dbt command."""
    cmd = ["dbt", command, "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR]
    logger.info(f"Running: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise AirflowException(f"dbt {command} failed: {result.stderr}")

    logger.info(f"dbt {command} completed successfully")


with DAG(
    dag_id="platform_pipeline",
    default_args=default_args,
    description="End-to-end taxi data platform pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["platform", "taxi", "end-to-end"],
) as dag:

    start_streaming = PythonOperator(
        task_id="start_streaming",
        python_callable=scale_streaming,
        op_kwargs={"replicas": 1},
    )

    accumulate_data = PythonOperator(
        task_id="accumulate_data",
        python_callable=wait_for_data,
    )

    stop_streaming = PythonOperator(
        task_id="stop_streaming",
        python_callable=scale_streaming,
        op_kwargs={"replicas": 0},
    )

    spark_processing = PythonOperator(
        task_id="spark_processing",
        python_callable=run_spark_job,
    )

    load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=load_to_staging,
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt,
        op_kwargs={"command": "run"},
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt,
        op_kwargs={"command": "test"},
    )

    # Full pipeline flow
    start_streaming >> accumulate_data >> stop_streaming >> spark_processing >> load_staging >> dbt_run >> dbt_test
