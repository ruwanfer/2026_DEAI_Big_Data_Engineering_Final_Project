# NYC TLC Taxi Analytics Pipeline

> **Big Data Engineering — Final Project 2026 | DEAI**  
> Deadline: 2026-03-31

A full end-to-end big data engineering pipeline for New York City taxi trip data (January–April 2025), built with Apache Spark, MinIO, DuckDB, and FastAPI following the **Medallion Architecture** (raw → silver → gold).

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [Data Lake Structure](#data-lake-structure)
- [Pipeline Stages](#pipeline-stages)
- [API Endpoints](#api-endpoints)
- [Dashboard](#dashboard)
- [How to Run](#how-to-run)
- [Team](#team)

---

## Project Overview

This project implements a big data pipeline that:

1. **Ingests** raw NYC TLC Yellow and Green taxi trip records from the public AWS CloudFront CDN
2. **Cleans and enriches** the data using an 8-step Apache Spark pipeline (silver layer)
3. **Aggregates** the cleaned data into 6 pre-computed analytics tables (gold layer)
4. **Stores** all data in a layered MinIO S3-compatible object storage (data lake)
5. **Serves** analytics results through a FastAPI REST API backed by DuckDB
6. **Visualises** results in an interactive HTML dashboard

**Dataset:** NYC TLC High Volume For-Hire Vehicle Trip Records, January 2025 – March 2025  
**Total files:** 3 Parquet files

---

## Architecture

```
NYC TLC
        │
        ▼  Python download
MinIO  ──►  taxi/raw/
        │
        ▼  Spark silver pipeline (clean + feature engineering)
MinIO  ──►  taxi/silver/   (partitioned by pickup_month)
        │
        ▼  Spark gold pipeline (aggregations + dimension joins)
MinIO  ──►  taxi/gold/     (6 analytics tables)
        │
        ▼  DuckDB (SQL over S3 Parquet via httpfs)
FastAPI ──►  REST API  ──►  http://localhost:8000/docs
        │
        ▼
HTML Dashboard  ──►  file:///dashboard.html
```

---

## Tech Stack

| Component | Version | Role |
|-----------|---------|------|
| Apache Spark | 4.1.1 | Distributed data processing — cleaning, feature engineering, aggregations |
| MinIO | Docker (latest) | S3-compatible object storage — hosts all data layers |
| Python | 3.13 / Conda | Pipeline orchestration and scripts |
| hadoop-aws | 3.4.2 | S3A connector — Spark reads/writes MinIO over S3 protocol |
| DuckDB | 1.1.3 | In-process SQL engine — FastAPI reads gold Parquet files |
| FastAPI | 0.115.0 | REST API layer — exposes analytics endpoints |
| Uvicorn | 0.30.6 | ASGI server for FastAPI |
| Pandas | 2.2.3 | Data manipulation in pipeline scripts |
| OpenJDK | 21 | JVM runtime for Apache Spark |
| Ubuntu | 24 VM | Runtime platform (VMware Workstation) |

---

## Repository Structure

```
nyc-tlc-taxi-pipeline/
│
├── pipeline/
│   ├── Download_raw.py        # Phase 1 — download raw Parquet from NYC TLC CDN
│   ├── Upload_minio.py        # Phase 1 — upload raw files to MinIO with validation
│   ├── Verifysparkread.py     # Phase 1 — verify Spark can read from MinIO via S3A
│   ├── silverLayer.py         # Phase 2 — 8-step Spark cleaning + feature engineering
│   └── goldlayer.py           # Phase 2 — Spark aggregations + dimension joins
│
├── api/
│   ├── API.py                # FastAPI application — all endpoints
│   └── requirements.txt       # Python dependencies
│
├── dashboard/
│   └── nyc_tlc_dashboard.html # Interactive analytics dashboard
│
├── import_gold.py             # Import gold export into MinIO (for teammates)
└── README.md
```

---

## Data Lake Structure

All data lives in a single MinIO bucket named `taxi`:

```
taxi/
├── raw/
│   ├── yellow/                # Original Yellow taxi Parquet files
│   └── green/                 # Original Green taxi Parquet files
│
├── silver/
│   ├── yellow/                # Cleaned + feature-engineered, partitioned by pickup_month
│   └── green/
│
├── gold/
│   ├── rides_per_day/         # Daily ride counts with calendar flags
│   ├── hourly_demand/         # Trip volume by hour and day of week
│   ├── provider_summary/      # Daily KPIs per dataset
│   ├── top_routes/            # Top 20 pickup→dropoff corridors
│   └── wait_time_stats/       # Wait time for customers for each cab company for location
│   
│
├── dimensions/
│   ├── zones/                 # Taxi zone lookup (263 NYC zones)
│   └── calendar/              # Jan–Apr 2025 calendar with holiday flags
│
└── audit/
    └── unknown_locations/     # Quarantined rows with unknown location IDs
```

---

## Pipeline Stages

### Phase 1 — Data Ingestion

- Raw Parquet files downloaded from `https://d37ci6vzurychx.cloudfront.net/trip-data/`
- Validated with PyArrow before upload
- Uploaded to `s3://taxi/raw/` via boto3
- Spark reads using S3A connector with manually downloaded JARs (hadoop-aws 3.4.2 + aws-java-sdk-bundle 1.12.262)

### Phase 2 — Silver Layer (Data Cleaning)

8-step Spark pipeline:

1. Schema enforcement — cast columns to correct types
2. Standardise columns — lowercase, rename lpep/tpep to unified names
3. Drop critical nulls — pickup, dropoff, location IDs, fare
4. Date alignment — keep only 2025 rows (removes GPS resets to 1970/2001)
5. Logical validation — dropoff must be strictly after pickup
6. Deduplication — remove exact duplicate rows
7. Threshold filters — distance (0.1–100 mi), fare ($2.50–$1000), duration (1–360 min)
8. Feature engineering — add `trip_duration_min`, `pickup_hour`, `pickup_day`, `pickup_month`, `pickup_date`

### Phase 2 — Gold Layer (Aggregations)

Six pre-aggregated analytics tables produced by joining silver data with dimension tables:

| Table | Grain | Key Metrics |
|-------|-------|-------------|
| `rides_per_day` | date × company | total_rides, is_weekday, is_holiday, day_name |
| `hourly_demand` | hour × day_of_week × company | total_rides |
| `provider_summary` | date × company | avg_distance, avg_fare, total_net_revenue, avg_driver_pay, avg_slow_zone_index |
| `top_routes` | pickup × dropoff × company | trip_count, rank (Top 20), zone_names, boroughs |
| `fare_stats` | hour × month × company | trip_count, avg/min/max fare, avg_tip, avg_tip_pct, avg_driver_pay |
| `wait_time_stats` | hour × month × pickup_zone × company | trip_count, avg_wait_min, min_wait_min, max_wait_min |

---

## API Endpoints

Start the API:
```bash
pip install -r requirements.txt
uvicorn API:app --reload --port 8000
```

Interactive docs: **http://localhost:8000/docs**

| Endpoint | Tag | Description |
|----------|-----|-------------|
| `GET /health` | Health | MinIO connectivity check |
| `GET /rides-per-day` | Demand | Daily ride counts with holiday flags |
| `GET /hourly-demand` | Demand | Trip volume by hour and day of week |
| `GET /provider-summary` | Provider | Daily KPIs per dataset |
| `GET /top-routes` | Routes | Top 20 pickup→dropoff corridors |
| `GET /fare-stats` | Fares | Fare and tip statistics |
| `GET /zone-stats` | Zones | Daily stats per taxi zone |
| `GET /analysis/peak-hours` | Analysis | Busiest and quietest hours |
| `GET /analysis/yellow-vs-green` | Analysis | Yellow vs Green comparison |
| `GET /analysis/slow-zones` | Analysis | Most congested zones |
| `GET /analysis/holiday-impact` | Analysis | Holiday vs weekday demand |

---

## Dashboard

Open `nyc_tlc_dashboard.html` in a browser while the API is running.

Charts included:
- Summary KPIs (total rides, yellow rides, green rides, avg fare)
- Rides per day over time — line chart
- Peak hours — bar chart by hour of day
- Holiday impact — weekday vs weekend vs holiday
- Yellow vs Green comparison — fare, distance, slow-zone index
- Top 10 routes — horizontal bar chart
- Top 15 slow zones — congestion ranking

---

## How to Run

### Prerequisites

- Ubuntu 24 VM (VMware)
- Docker installed
- Conda / Python 3.13
- OpenJDK 21

### Step 1 — Start MinIO

```bash
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"
```

MinIO console: http://localhost:9001

### Step 2 — Download and upload raw data

```bash
python pipeline/Download_raw.py
python pipeline/Upload_minio.py
python pipeline/Verifysparkread.py
```

### Step 3 — Run Spark pipelines

```bash
python pipeline/silverLayer.py
python pipeline/goldlayer.py
```

### Step 4 — Start the API

```bash
cd api
pip install -r requirements.txt
uvicorn API:app --reload --port 8000
```

### Step 5 — Open the dashboard

Open `dashboard/nyc_tlc_dashboard.html` in Firefox:
```bash
firefox file:///path/to/nyc_tlc_dashboard.html
```

### For teammates — import gold data directly

If you only need to run the API without running Spark:

```bash
unzip gold_export.zip -d gold_export
python import_gold.py
```

---

## Team

| Name | Role |
|------|------|
| Chamith Priyamal | Data ingestion, Spark silver pipeline, Spark gold pipeline, data lake structure |
| Ruwan Fernando | FastAPI layer, DuckDB integration, data analysis endpoints, dashboard |

---

**Course:** Big Data Engineering (DEAI)  
**Year:** 2026  
**Dataset:** NYC TLC Trip Record Data — https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
