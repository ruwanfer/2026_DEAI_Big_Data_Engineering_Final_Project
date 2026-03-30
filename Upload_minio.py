
#Phase 1 - Step 2: Upload HVFHV Parquet files to MinIO


import boto3
import pyarrow.parquet as pq
from botocore.client import Config
from pathlib import Path
import sys

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS   = "minioadmin"
MINIO_SECRET   = "minioadmin123"
BUCKET         = "nyc-taxi"
RAW_DIR        = Path("./raw_data/hvfhv")
S3_PREFIX      = "raw/hvfhv"


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3):
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)
        print(f"  [bucket] created '{BUCKET}'")
    else:
        print(f"  [bucket] '{BUCKET}' already exists")


def already_uploaded(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


def validate_parquet(path: Path) -> bool:
    try:
        meta = pq.read_metadata(path)
        print(f"  [validate] {path.name}: {meta.num_rows:,} rows")
        return True
    except Exception as e:
        print(f"  [invalid] {path.name}: {e}")
        return False


def upload_file(s3, local_path: Path, s3_key: str) -> bool:
    if already_uploaded(s3, s3_key):
        print(f"  [skip] s3://{BUCKET}/{s3_key} already exists")
        return True

    size_mb = local_path.stat().st_size / 1_048_576
    print(f"  [upload] {local_path.name} ({size_mb:.1f} MB) → s3://{BUCKET}/{s3_key}")
    try:
        s3.upload_file(str(local_path), BUCKET, s3_key)
        print(f"  [ok] uploaded")
        return True
    except Exception as e:
        print(f"  [error] {e}")
        return False


def main():
    print("=" * 50)
    print("  MinIO Upload: nyc-taxi/raw/hvfhv/")
    print("=" * 50)

    if not RAW_DIR.exists():
        print(f"\n[error] {RAW_DIR} not found — run download_raw.py first")
        sys.exit(1)

    s3 = get_client()
    ensure_bucket(s3)

    parquet_files = sorted(RAW_DIR.glob("*.parquet"))
    if not parquet_files:
        print(f"\n[error] No parquet files found in {RAW_DIR}")
        sys.exit(1)

    print(f"\n  Found {len(parquet_files)} file(s) to upload\n")

    results = []
    for pfile in parquet_files:
        valid = validate_parquet(pfile)
        if not valid:
            results.append((pfile.name, False))
            continue
        s3_key = f"{S3_PREFIX}/{pfile.name}"
        ok = upload_file(s3, pfile, s3_key)
        results.append((pfile.name, ok))

    print("\n[Summary]")
    for fname, ok in results:
        status = "OK" if ok else "FAILED"
        print(f"  [{status}] {fname}")

    failed = [r for r in results if not r[1]]
    if not failed:
        print(f"\n  All files in s3://{BUCKET}/{S3_PREFIX}/")


if __name__ == "__main__":
    main()