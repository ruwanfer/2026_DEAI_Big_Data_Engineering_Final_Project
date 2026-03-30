
#Phase 2 - Silver Layer Pipeline

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType, StringType, LongType
from datetime import datetime

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS   = "minioadmin"
MINIO_SECRET   = "minioadmin123"
BUCKET         = "nyc-taxi"
JAR_DIR        = "/home/chamith/Bigdata_project/raw_data/venv/lib/python3.13/site-packages/pyspark/jars"

RAW_PATH    = f"s3a://{BUCKET}/raw/hvfhv/*.parquet"
SILVER_PATH = f"s3a://{BUCKET}/silver/hvfhv"
AUDIT_PATH  = f"s3a://{BUCKET}/audit/unknown_locations/hvfhv"
TEMP_DIR    = "/home/chamith/spark_temp"
JAR_DIR        = "/home/chamith/Bigdata_project/raw_data/venv/lib/python3.13/site-packages/pyspark/jars"

MIN_DISTANCE   = 0.1
MAX_DISTANCE   = 250.0
MIN_FARE       = 2.50
MAX_FARE       = 3000.0
MIN_DURATION   = 1.0
MAX_DURATION   = 360.0
MAX_WAIT       = 60.0
MIN_LOCATION   = 1
MAX_LOCATION   = 263
VALID_YEAR     = 2025

COMPANY_MAP = {
    "HV0002": "Juno",
    "HV0003": "Uber",
    "HV0004": "Via",
    "HV0005": "Lyft",
}


def build_spark() -> SparkSession:
    # 1. Use compatible versions. 
    # For Hadoop 3.3.4, we usually need the corresponding aws-java-sdk-bundle
    spark_packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    
    packages_str = ",".join(spark_packages)

    return (
        SparkSession.builder
        .appName("Silver-HVFHV")
        .master("local[*]")
        .config("spark.local.dir",              TEMP_DIR)
        .config("spark.driver.memory",          "16g")
        .config("spark.executor.memory",        "4g")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size",    "2g")
        .config("spark.jars.packages",          packages_str)
        # --- CRITICAL FIXES BELOW ---
        # Explicitly set the credentials provider to use simple Access/Secret keys
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.timeout",           "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime",        "60")
        .config("spark.hadoop.fs.s3a.multipart.purge.age",          "86400")
        # ----------------------------
        .getOrCreate()
    )
# Step 1: Cast core columns to correct types
def enforce_schema(df: DataFrame) -> DataFrame:
    casts = {
        "hvfhs_license_num":    StringType(),
        "dispatching_base_num": StringType(),
        "originating_base_num": StringType(),
        "request_datetime":     TimestampType(),
        "on_scene_datetime":    TimestampType(),
        "pickup_datetime":      TimestampType(),
        "dropoff_datetime":     TimestampType(),
        "pulocationid":         LongType(),
        "dolocationid":         LongType(),
        "trip_miles":           DoubleType(),
        "trip_time":            LongType(),
        "base_passenger_fare":  DoubleType(),
        "tolls":                DoubleType(),
        "bcf":                  DoubleType(),
        "sales_tax":            DoubleType(),
        "congestion_surcharge": DoubleType(),
        "tips":                 DoubleType(),
        "driver_pay":           DoubleType(),
    }
    for col_name, dtype in casts.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))
    return df


# Step 2: Standardise column names to lowercase and consistent naming
def standardise_columns(df: DataFrame) -> DataFrame:
    df = df.toDF(*[c.lower() for c in df.columns])
    if "pulocationid" in df.columns:
        df = df.withColumnRenamed("pulocationid", "pickup_location_id")
    if "dolocationid" in df.columns:
        df = df.withColumnRenamed("dolocationid", "dropoff_location_id")
    return df


# Step 3: Drop rows with null values in critical columns
def drop_critical_nulls(df: DataFrame) -> DataFrame:
    critical = [
        "hvfhs_license_num",
        "request_datetime",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_miles",
        "base_passenger_fare",
    ]
    return df.dropna(subset=critical)


# Step 4: Keep only rows where pickup year matches processing year
def align_dates(df: DataFrame) -> DataFrame:
    return df.filter(F.year("pickup_datetime") == VALID_YEAR)


# Step 5: Ensure dropoff is strictly after pickup
def validate_logic(df: DataFrame) -> DataFrame:
    return df.filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))


# Step 6: Remove exact duplicate rows
def deduplicate(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()


# Step 7: Apply threshold filters — returns (clean_df, audit_df)
def apply_thresholds(df: DataFrame) -> tuple:
    # Compute duration before filtering so we can apply duration threshold
    df = df.withColumn(
        "trip_duration_min",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0
    )

    # Compute wait time: from ride request to actual pickup
    df = df.withColumn(
        "wait_time_min",
        F.when(
            F.col("request_datetime").isNotNull(),
            (F.unix_timestamp("pickup_datetime") - F.unix_timestamp("request_datetime")) / 60.0
        ).otherwise(None)
    )

    # Rows with unknown location IDs go to audit instead of being dropped
    unknown_loc = (
        (F.col("pickup_location_id")  > MAX_LOCATION) |
        (F.col("dropoff_location_id") > MAX_LOCATION)
    )
    audit_df = df.filter(unknown_loc)
    df       = df.filter(~unknown_loc)

    # Valid location ID range
    df = df.filter(
        (F.col("pickup_location_id")  >= MIN_LOCATION) &
        (F.col("dropoff_location_id") >= MIN_LOCATION)
    )

    # Trip distance
    df = df.filter(
        (F.col("trip_miles") >= MIN_DISTANCE) &
        (F.col("trip_miles") <= MAX_DISTANCE)
    )

    # Fare
    df = df.filter(
        (F.col("base_passenger_fare") >= MIN_FARE) &
        (F.col("base_passenger_fare") <= MAX_FARE)
    )

    # Duration
    df = df.filter(
        (F.col("trip_duration_min") >= MIN_DURATION) &
        (F.col("trip_duration_min") <= MAX_DURATION)
    )

    # Wait time — only filter if wait time is present
    df = df.filter(
        F.col("wait_time_min").isNull() |
        ((F.col("wait_time_min") >= 0) & (F.col("wait_time_min") <= MAX_WAIT))
    )

    return df, audit_df


# Step 8: Add time-based features and company name
def add_features(df: DataFrame) -> DataFrame:
    # Map hvfhs_license_num to human-readable company name
    company_map_expr = F.create_map(
        *[item for pair in COMPANY_MAP.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )

    return (
        df
        .withColumn("company",      company_map_expr[F.col("hvfhs_license_num")])
        .withColumn("pickup_hour",  F.hour("pickup_datetime"))
        .withColumn("pickup_day",   F.dayofweek("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
        .withColumn("pickup_date",  F.to_date("pickup_datetime"))
        .withColumn("net_revenue",
            F.col("base_passenger_fare")
            - F.coalesce(F.col("tolls"),                F.lit(0))
            - F.coalesce(F.col("bcf"),                  F.lit(0))
            - F.coalesce(F.col("sales_tax"),            F.lit(0))
            - F.coalesce(F.col("congestion_surcharge"), F.lit(0))
            - F.coalesce(F.col("cbd_congestion_fee"),   F.lit(0))
        )
    )


# Step 9: Write to MinIO
def write_silver(df: DataFrame):
    (
        df.write
        .mode("overwrite")
        .partitionBy("pickup_month")
        .parquet(SILVER_PATH)
    )


def write_audit(df: DataFrame):
    if not df.rdd.isEmpty():
        df.write.mode("overwrite").parquet(AUDIT_PATH)


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    start = datetime.now()
    print("\n" + "=" * 52)
    print("  SILVER PIPELINE — HVFHV")
    print("=" * 52)

    print(f"\n  [1/8] Reading raw HVFHV Parquet from MinIO...")
    df = spark.read.parquet(RAW_PATH)
    raw_count = df.count()
    print(f"        raw rows: {raw_count:,}")

    print("  [2/8] Enforcing schema...")
    df = enforce_schema(df)

    print("  [3/8] Standardising column names...")
    df = standardise_columns(df)

    print("  [4/8] Dropping critical nulls...")
    df = drop_critical_nulls(df)

    print("  [5/8] Aligning dates to 2025...")
    df = align_dates(df)

    print("  [6/8] Validating dropoff > pickup...")
    df = validate_logic(df)

    print("  [7/8] Deduplicating...")
    df = deduplicate(df)

    print("  [8/8] Applying threshold filters...")
    df, audit_df = apply_thresholds(df)
    audit_count  = audit_df.count()

    print("        Adding features...")
    df = add_features(df)
    silver_count = df.count()

    print(f"\n  Writing silver → {SILVER_PATH}")
    write_silver(df)

    print(f"  Writing audit  → {AUDIT_PATH} ({audit_count:,} rows)")
    write_audit(audit_df)

    elapsed = (datetime.now() - start).seconds
    dropped = raw_count - silver_count - audit_count
    pct     = (silver_count / raw_count * 100) if raw_count > 0 else 0

    print("\n" + "=" * 52)
    print(f"  SILVER COMPLETE  ({elapsed}s)")
    print("=" * 52)
    print(f"  Raw rows     : {raw_count:,}")
    print(f"  Silver rows  : {silver_count:,}  ({pct:.1f}% kept)")
    print(f"  Audit rows   : {audit_count:,}")
    print(f"  Dropped rows : {dropped:,}")

    spark.stop()


if __name__ == "__main__":
    main()