#Phase 2 - Gold Layer Pipeline (HVFHV)

import io
import holidays
import pandas as pd
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
 
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS   = "minioadmin"
MINIO_SECRET   = "minioadmin123"
BUCKET         = "nyc-taxi"
JAR_DIR        = "/home/chamith/Bigdata_project/raw_data/venv/lib/python3.13/site-packages/pyspark/jars"
 
SILVER_PATH = f"s3a://{BUCKET}/silver/hvfhv"
DIM_ZONES   = f"s3a://{BUCKET}/dimensions/zones/taxi_zone_lookup.csv"
TEMP_DIR    = "/home/chamith/spark_temp"
 
TOP_ROUTES_N = 20
MIN_TIP_PCT  = 0.0
MAX_TIP_PCT  = 0.50
DATE_START   = "2025-01-01"
DATE_END     = "2025-03-31"
 
GOLD = {
    "rides_per_day":    f"s3a://{BUCKET}/gold/rides_per_day",
    "hourly_demand":    f"s3a://{BUCKET}/gold/hourly_demand",
    "provider_summary": f"s3a://{BUCKET}/gold/provider_summary",
    "top_routes":       f"s3a://{BUCKET}/gold/top_routes",
    "fare_stats":       f"s3a://{BUCKET}/gold/fare_stats",
    "wait_time_stats":  f"s3a://{BUCKET}/gold/wait_time_stats",
}
 
 
def build_spark() -> SparkSession:
    spark_packages = ["org.apache.hadoop:hadoop-aws:3.3.4", "com.amazonaws:aws-java-sdk-bundle:1.12.262"]
    
    return (
        SparkSession.builder
        .appName("Silver-HVFHV-32GB-Power")
        .master("local[*]")
        .config("spark.local.dir", TEMP_DIR)
        
        # --- MEMORY UPGRADE ---
        .config("spark.driver.memory", "28g")
        .config("spark.executor.memory", "28g")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "4g")
        # ----------------------

        .config("spark.jars.packages", ",".join(spark_packages))
        
        # MinIO/S3A Connection
        .config("spark.hadoop.fs.s3a.endpoint",                   MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",                 MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",                 MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access",         "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",    "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        # Numeric Overrides
        .config("spark.hadoop.fs.s3a.connection.timeout",           "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime",        "60")
        .config("spark.hadoop.fs.s3a.multipart.purge.age",          "86400")
        
        # Shuffle Tuning for high memory
        .config("spark.sql.shuffle.partitions",                   "100") 
        .config("spark.sql.parquet.enableVectorizedReader",       "false")
        .getOrCreate()
    )
 
 
# Build calendar dimension for Jan-Mar 2025 and upload to MinIO
def build_calendar(spark: SparkSession) -> DataFrame:
    print("  Building calendar dimension...")
    ny_holidays = holidays.US(state="NY", years=2025)
    dates = pd.date_range(DATE_START, DATE_END)
 
    rows = []
    for d in dates:
        d_date = d.date()
        rows.append({
            "date":         d_date,
            "is_weekday":   d.dayofweek < 5,
            "is_holiday":   d_date in ny_holidays,
            "holiday_name": ny_holidays.get(d_date, None),
            "day_name":     d.day_name(),
            "month_name":   d.month_name(),
            "week_number":  int(d.isocalendar().week),
        })
 
    pdf = pd.DataFrame(rows)
 
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    buf = io.BytesIO()
    pdf.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(
        Bucket=BUCKET,
        Key="dimensions/calendar/calendar.parquet",
        Body=buf.getvalue(),
    )
    print(f"  Calendar uploaded: {len(pdf)} rows")
    return spark.createDataFrame(pdf)
 
 
# Load taxi zone lookup from MinIO
def load_zones(spark: SparkSession) -> DataFrame:
    print("  Loading taxi zone lookup...")

    # Read CSV from MinIO via boto3 into pandas first
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    obj = s3.get_object(Bucket=BUCKET, Key="dimensions/zones/taxi_zone_lookup.csv")
    pdf = pd.read_csv(io.BytesIO(obj["Body"].read()))

    # Standardise column names
    pdf.columns = [c.strip().lower().replace(" ", "_") for c in pdf.columns]
    pdf = pdf.rename(columns={
        "locationid":    "location_id",
        "zone":          "zone_name",
        "borough":       "borough",
        "service_zone":  "service_zone",
    })
    pdf = pdf[["location_id", "zone_name", "borough", "service_zone"]]

    df = spark.createDataFrame(pdf)
    print(f"  Zones loaded: {df.count()} zones")
    return df
 
 
# Daily trip count per company enriched with weekday and holiday flags
def build_rides_per_day(df: DataFrame, calendar: DataFrame) -> DataFrame:
    agg = (
        df.groupBy("pickup_date", "hvfhs_license_num", "company")
        .agg(F.count("*").alias("total_rides"))
    )
    return agg.join(
        calendar.select("date", "is_weekday", "is_holiday", "day_name", "month_name"),
        agg["pickup_date"] == calendar["date"],
        how="left"
    ).drop("date")
 
 
# Trip counts by hour and day of week per company
def build_hourly_demand(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("pickup_hour", "pickup_day", "hvfhs_license_num", "company")
        .agg(F.count("*").alias("total_rides"))
        .orderBy("hvfhs_license_num", "pickup_day", "pickup_hour")
    )
 
 
# Daily KPIs per company — fare, distance, duration, net revenue, slow zone index
def build_provider_summary(df: DataFrame, calendar: DataFrame) -> DataFrame:
    df = df.withColumn(
        "slow_zone_index",
        F.when(F.col("trip_miles") > 0,
               F.col("trip_duration_min") / F.col("trip_miles")
        ).otherwise(None)
    )
 
    agg = (
        df.groupBy("pickup_date", "hvfhs_license_num", "company")
        .agg(
            F.count("*").alias("total_rides"),
            F.round(F.avg("trip_miles"),          2).alias("avg_distance_miles"),
            F.round(F.avg("trip_duration_min"),   2).alias("avg_duration_min"),
            F.round(F.avg("base_passenger_fare"), 2).alias("avg_fare"),
            F.round(F.sum("net_revenue"),         2).alias("total_net_revenue"),
            F.round(F.avg("driver_pay"),          2).alias("avg_driver_pay"),
            F.round(F.avg("slow_zone_index"),     4).alias("avg_slow_zone_index"),
        )
    )
 
    return agg.join(
        calendar.select("date", "is_weekday", "is_holiday", "day_name"),
        agg["pickup_date"] == calendar["date"],
        how="left"
    ).drop("date")
 
 
# Top 20 pickup/dropoff zone pairs per company enriched with zone names
def build_top_routes(df: DataFrame, zones: DataFrame) -> DataFrame:
    route_counts = (
        df.groupBy("pickup_location_id", "dropoff_location_id", "hvfhs_license_num", "company")
        .agg(F.count("*").alias("trip_count"))
    )
 
    window = Window.partitionBy("hvfhs_license_num").orderBy(F.desc("trip_count"))
    ranked = (
        route_counts
        .withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") <= TOP_ROUTES_N)
    )
 
    pickup_zones = zones.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("zone_name").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )
    dropoff_zones = zones.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("zone_name").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )
 
    return (
        ranked
        .join(pickup_zones,  on="pickup_location_id",  how="left")
        .join(dropoff_zones, on="dropoff_location_id", how="left")
        .orderBy("hvfhs_license_num", "rank")
    )
 
 
# Hourly fare and tip statistics per company
def build_fare_stats(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "tip_pct",
        F.when(F.col("base_passenger_fare") > 0,
               F.col("tips") / F.col("base_passenger_fare")
        ).otherwise(None)
    )
    df = df.filter(
        F.col("tip_pct").isNull() |
        ((F.col("tip_pct") > MIN_TIP_PCT) & (F.col("tip_pct") <= MAX_TIP_PCT))
    )
 
    return (
        df.groupBy("pickup_hour", "pickup_month", "hvfhs_license_num", "company")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("base_passenger_fare"), 2).alias("avg_fare"),
            F.round(F.min("base_passenger_fare"), 2).alias("min_fare"),
            F.round(F.max("base_passenger_fare"), 2).alias("max_fare"),
            F.round(F.avg("tips"),                2).alias("avg_tip"),
            F.round(F.avg("tip_pct") * 100,       2).alias("avg_tip_pct"),
            F.round(F.avg("driver_pay"),           2).alias("avg_driver_pay"),
        )
        .orderBy("hvfhs_license_num", "pickup_month", "pickup_hour")
    )
 
 
# Wait time analysis by company, hour, and pickup zone
def build_wait_time_stats(df: DataFrame, zones: DataFrame) -> DataFrame:
    wait_df = df.filter(F.col("wait_time_min").isNotNull())
 
    agg = (
        wait_df.groupBy("pickup_hour", "pickup_month", "pickup_location_id",
                         "hvfhs_license_num", "company")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("wait_time_min"), 2).alias("avg_wait_min"),
            F.round(F.min("wait_time_min"), 2).alias("min_wait_min"),
            F.round(F.max("wait_time_min"), 2).alias("max_wait_min"),
        )
    )
 
    zone_lookup = zones.select(
        F.col("location_id").alias("pickup_location_id"),
        "zone_name", "borough"
    )
 
    return (
        agg
        .join(zone_lookup, on="pickup_location_id", how="left")
        .orderBy("hvfhs_license_num", "pickup_month", "pickup_hour")
    )
 
 
def write_gold(df: DataFrame, path: str, name: str):
    print(f"  Writing {name} → {path}")
    df.write.mode("overwrite").parquet(path)
    print(f"  Done: {name}")
 
 
def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
 
    start = datetime.now()
    print("\n" + "=" * 52)
    print("  GOLD PIPELINE — HVFHV")
    print("=" * 52)
 
    print("\n[Dimensions]")
    calendar = build_calendar(spark)
    zones    = load_zones(spark)
 
    print(f"\n[Silver read]")
    print(f"  Reading {SILVER_PATH}")
    silver = spark.read.parquet(SILVER_PATH)
    total  = silver.count()
    print(f"  Silver rows: {total:,}")
 
    # Cache silver — reused across all 6 aggregations
    silver.cache()
 
    print("\n[Gold tables]")
 
    write_gold(build_rides_per_day(silver, calendar),   GOLD["rides_per_day"],    "rides_per_day")
    write_gold(build_hourly_demand(silver),             GOLD["hourly_demand"],    "hourly_demand")
    write_gold(build_provider_summary(silver, calendar),GOLD["provider_summary"], "provider_summary")
    write_gold(build_top_routes(silver, zones),         GOLD["top_routes"],       "top_routes")
    write_gold(build_fare_stats(silver),                GOLD["fare_stats"],       "fare_stats")
    write_gold(build_wait_time_stats(silver, zones),    GOLD["wait_time_stats"],  "wait_time_stats")
 
    silver.unpersist()
 
    elapsed = (datetime.now() - start).seconds
    print("\n" + "=" * 52)
    print(f"  GOLD COMPLETE  ({elapsed}s)")
    print("=" * 52)
    for name, path in GOLD.items():
        print(f"  {name:<20} {path}")
 
    spark.stop()
 
 
if __name__ == "__main__":
    main()