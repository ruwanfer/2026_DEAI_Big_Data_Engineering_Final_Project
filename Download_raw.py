
#Phase 1 - Step 1: Download HVFHV raw Parquet files


import os
import requests
from pathlib import Path

BASE_URL   = "https://d37ci6vzurychx.cloudfront.net/trip-data"
OUTPUT_DIR = Path("./raw_data/hvfhv")
MONTHS     = ["2025-01", "2025-02", "2025-03"]


def download_file(url: str, dest: Path) -> bool:
    if dest.exists():
        print(f"  [skip] {dest.name} already exists")
        return True

    print(f"  [download] {url}")
    try:
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
        size_mb = dest.stat().st_size / 1_048_576
        print(f"  [ok] {dest.name} ({size_mb:.1f} MB)")
        return True
    except requests.HTTPError as e:
        print(f"  [error] {e}")
        return False


def main():
    print("=" * 50)
    print("  HVFHV Parquet Downloader")
    print("=" * 50)

    results = []
    for month in MONTHS:
        filename = f"fhvhv_tripdata_{month}.parquet"
        url      = f"{BASE_URL}/{filename}"
        dest     = OUTPUT_DIR / filename
        ok       = download_file(url, dest)
        results.append((month, filename, ok))

    print("\n[Summary]")
    for month, fname, ok in results:
        status = "OK" if ok else "FAILED"
        print(f"  [{status}] {fname}")

    failed = [r for r in results if not r[2]]
    if failed:
        print(f"\n  {len(failed)} file(s) failed — re-run to retry")
    else:
        total_mb = sum(
            (OUTPUT_DIR / f"fhvhv_tripdata_{m}.parquet").stat().st_size
            for m, _, ok in results if ok
        ) / 1_048_576
        print(f"\n  All files downloaded to {OUTPUT_DIR.resolve()}")
        print(f"  Total size: {total_mb:.1f} MB")


if __name__ == "__main__":
    main()