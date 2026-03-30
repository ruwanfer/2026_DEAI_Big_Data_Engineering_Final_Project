
#Phase 1 - Step 3: Verify Spark reads HVFHV data from MinIO 

from pyspark.sql import SparkSession

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS   = "minioadmin"
MINIO_SECRET   = "minioadmin123"
BUCKET         = "nyc-taxi"
JAR_DIR        = "/home/chamith/Bigdata_project/raw_data/venv/lib/python3.13/site-packages/pyspark/jars"


def build_spark() -> SparkSession:
    
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    
    return (
        SparkSession.builder
        .appName("Silver-HVFHV")
        .master("local[*]")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.hadoop.fs.s3a.endpoint",                   MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",                 MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",                 MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access",         "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",    "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.timeout",         "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime",      "60")
        .config("spark.hadoop.fs.s3a.multipart.purge.age",          "86400")
        .config("spark.sql.parquet.enableVectorizedReader",       "false")
        .config("spark.sql.shuffle.partitions",                   "8")
        .getOrCreate()
    )


def main():
    print("=" * 50)
    print("  Spark S3A Read Verification")
    print("=" * 50)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    path = f"s3a://{BUCKET}/raw/hvfhv/*.parquet"
    print(f"\n  Reading: {path}\n")

    try:
        df = spark.read.parquet(path)
        count = df.count()
        cols  = len(df.columns)

        print(f"  Rows    : {count:,}")
        print(f"  Columns : {cols}")
        print(f"\n  Schema (first 10 columns):")
        for field in df.schema.fields[:10]:
            print(f"    {field.name:<35} {field.dataType}")

        print(f"\n  Key HVFHV columns present:")
        key_cols = [
            "hvfhs_license_num", "dispatching_base_num",
            "request_datetime", "pickup_datetime", "dropoff_datetime",
            "trip_miles", "trip_time", "base_passenger_fare",
            "tolls", "tips", "driver_pay", "congestion_surcharge",
            "cbd_congestion_fee"
        ]
        for col in key_cols:
            status = "YES" if col in df.columns else "NOT FOUND"
            print(f"    [{status}] {col}")

    except Exception as e:
        print(f"  [error] {e}")

    spark.stop()
    print("\n  Verification complete.")


if __name__ == "__main__":
    main()