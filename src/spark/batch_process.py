"""
Spark Batch Processing Job
Reads raw taxi ride Parquet files from S3, transforms them,
and writes aggregated results back to S3.
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localstack.data.svc.cluster.local:4566")
RAW_BUCKET = os.getenv("RAW_BUCKET", "platform-raw-data")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "platform-processed-data")


def create_spark_session():
    """Create Spark session configured for S3 (LocalStack)."""
    return (
        SparkSession.builder.appName("TaxiRideBatchProcessing")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def read_raw_data(spark):
    """Read raw taxi ride data from S3."""
    path = f"s3a://{RAW_BUCKET}/taxi-rides/"
    logger.info(f"Reading raw data from {path}")

    df = spark.read.parquet(path)
    count = df.count()
    logger.info(f"Read {count} raw records")
    return df


def clean_data(df):
    """Clean and validate raw data."""
    logger.info("Cleaning data...")

    cleaned = (
        df
        # Parse timestamp
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        # Filter invalid records
        .filter(F.col("trip_distance_miles") > 0)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("duration_minutes") > 0)
        # Derived columns
        .withColumn("fare_per_mile", F.round(F.col("fare_amount") / F.col("trip_distance_miles"), 2))
        .withColumn("fare_per_minute", F.round(F.col("fare_amount") / F.col("duration_minutes"), 2))
        .withColumn(
            "tip_percentage",
            F.round(F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount") * 100).otherwise(0), 2),
        )
        # Time-based features
        .withColumn(
            "is_rush_hour",
            F.when((F.col("event_hour").between(7, 9)) | (F.col("event_hour").between(16, 19)), True).otherwise(False),
        )
        .withColumn("is_weekend", F.when(F.dayofweek("event_ts").isin(1, 7), True).otherwise(False))
        # Drop raw timestamp string
        .drop("event_timestamp")
    )

    count = cleaned.count()
    logger.info(f"Cleaned data: {count} records")
    return cleaned


def compute_zone_aggregations(df):
    """Compute aggregations by pickup zone."""
    logger.info("Computing zone aggregations...")

    zone_stats = (
        df.groupBy("pickup_zone_id", "pickup_zone_name", "pickup_borough")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("total_amount"), 2).alias("avg_total"),
            F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
            F.round(F.avg("trip_distance_miles"), 2).alias("avg_distance"),
            F.round(F.avg("duration_minutes"), 2).alias("avg_duration"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.avg("passenger_count").cast("int").alias("avg_passengers"),
        )
        .withColumn("rank_by_revenue", F.row_number().over(Window.orderBy(F.desc("total_revenue"))))
    )

    logger.info(f"Zone aggregations: {zone_stats.count()} zones")
    return zone_stats


def compute_hourly_aggregations(df):
    """Compute aggregations by hour of day."""
    logger.info("Computing hourly aggregations...")

    hourly_stats = (
        df.groupBy("event_date", "event_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("total_amount"), 2).alias("avg_total"),
            F.round(F.sum("total_amount"), 2).alias("hourly_revenue"),
            F.round(F.avg("trip_distance_miles"), 2).alias("avg_distance"),
            F.round(F.avg("duration_minutes"), 2).alias("avg_duration"),
            F.sum("passenger_count").alias("total_passengers"),
        )
        .orderBy("event_date", "event_hour")
    )

    logger.info(f"Hourly aggregations: {hourly_stats.count()} rows")
    return hourly_stats


def compute_payment_analysis(df):
    """Compute payment type analysis."""
    logger.info("Computing payment analysis...")

    payment_stats = (
        df.groupBy("payment_type")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("total_amount"), 2).alias("avg_total"),
            F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        )
        .withColumn(
            "pct_of_trips", F.round(F.col("trip_count") / F.sum("trip_count").over(Window.partitionBy()) * 100, 2)
        )
    )

    return payment_stats


def write_results(df, name):
    """Write processed DataFrame to S3 as Parquet."""
    path = f"s3a://{PROCESSED_BUCKET}/{name}/"
    logger.info(f"Writing {name} to {path}")

    df.coalesce(1).write.mode("overwrite").parquet(path)
    logger.info(f"Successfully wrote {name}")


def main():
    """Main batch processing pipeline."""
    logger.info("=" * 60)
    logger.info("Starting Spark Batch Processing")
    logger.info("=" * 60)

    spark = create_spark_session()

    try:
        # Read raw data
        raw_df = read_raw_data(spark)

        # Clean and enrich
        cleaned_df = clean_data(raw_df)

        # Write cleaned data
        write_results(cleaned_df, "cleaned-rides")

        # Compute and write aggregations
        zone_stats = compute_zone_aggregations(cleaned_df)
        write_results(zone_stats, "zone-stats")

        hourly_stats = compute_hourly_aggregations(cleaned_df)
        write_results(hourly_stats, "hourly-stats")

        payment_stats = compute_payment_analysis(cleaned_df)
        write_results(payment_stats, "payment-stats")

        logger.info("=" * 60)
        logger.info("Batch processing complete!")
        logger.info("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
