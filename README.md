# NYC TLC Taxi Analytics Pipeline

> **Big Data Engineering — Final Project 2026 | DEAI**  
> Deadline: 2026-03-31

A full end-to-end big data engineering pipeline for New York City High Volume For-Hire Vehicle (HVFHV) trip data (January–March 2025), built with Apache Spark, MinIO, DuckDB, and FastAPI following the **Medallion Architecture** (raw → silver → gold).

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

1. **Ingests** raw NYC TLC HVFHV trip records (Uber, Lyft, Via, Juno) from the public AWS CloudFront CDN
2. **Cleans and enriches** the data using an 8-step Apache Spark pipeline (silver layer)
3. **Aggregates** the cleaned data into 6 pre-computed analytics tables (gold layer)
4. **Stores** all data in a layered MinIO S3-compatible object storage (data lake)
5. **Serves** analytics results through a FastAPI REST API backed by DuckDB
6. **Visualises** results in an interactive HTML dashboard

**Dataset:** NYC TLC High Volume For-Hire Vehicle (HVFHV) Trip Records — Uber, Lyft, Via, Juno  
**Period:** January 2025 – March 2025  
**Total files:** 3 Parquet files  
**MinIO Bucket:** `nyc-taxi`

---

## Architecture

```
NYC TLC CloudFront CDN
        │
        ▼  Python download (Download_raw.py)
MinIO  ──►  nyc-taxi/raw/hvfhv/
        │
        ▼  Spark silver pipeline (clean + feature engineering)
MinIO  ──►  nyc-taxi/silver/hvfhv/   (partitioned by pickup_month)
        │
        ▼  Spark gold pipeline (aggregations + dimension joins)
MinIO  ──►  nyc-taxi/gold/           (6 analytics tables)
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
| hadoop-aws | 3.3.4 | S3A connector — Spark reads/writes MinIO over S3 protocol |
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
├── Download_raw.py        # Phase 1 — download raw Parquet from NYC TLC CDN
├── Upload_minio.py        # Phase 1 — upload raw files to MinIO with validation
├── Verifysparkread.py     # Phase 1 — verify Spark can read from MinIO via S3A
├── silverLayer.py         # Phase 2 — 8-step Spark cleaning + feature engineering
├── goldlayer.py           # Phase 2 — Spark aggregations + dimension joins
├── API.py                 # FastAPI application — all 14 endpoints
├── dashboard.html         # Interactive analytics dashboard
├── requirements.txt       # Python dependencies
└── README.md
```

---

## Data Lake Structure

All data lives in a single MinIO bucket named `nyc-taxi`:

```
nyc-taxi/
├── raw/
│   └── hvfhv/                 # Original HVFHV Parquet files (Uber, Lyft, Via, Juno)
│
├── silver/
│   └── hvfhv/                 # Cleaned + feature-engineered, partitioned by pickup_month
│
├── gold/
│   ├── rides_per_day/         # Daily ride counts with calendar flags
│   ├── hourly_demand/         # Trip volume by hour and day of week
│   ├── provider_summary/      # Daily KPIs per company
│   ├── top_routes/            # Top 20 pickup→dropoff corridors per company
│   ├── fare_stats/            # Fare and tip statistics by hour and month
│   └── wait_time_stats/       # Wait time per company, zone, and hour
│
├── dimensions/
│   ├── zones/                 # Taxi zone lookup (263 NYC zones)
│   └── calendar/              # Jan–Mar 2025 calendar with holiday flags
│
└── audit/
    └── unknown_locations/     # Quarantined rows with unknown location IDs
```

---

## Pipeline Stages

### Phase 1 — Data Ingestion

- Raw Parquet files downloaded from `https://d37ci6vzurychx.cloudfront.net/trip-data/`
- Validated with PyArrow before upload
- Uploaded to `s3://nyc-taxi/raw/hvfhv/` via boto3
- Spark reads using S3A connector with manually downloaded JARs (hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262)

### Phase 2 — Silver Layer (Data Cleaning)

8-step Spark pipeline:

1. Schema enforcement — cast columns to correct types
2. Standardise columns — lowercase all column names, rename to unified naming
3. Drop critical nulls — pickup, dropoff, location IDs, fare, trip_miles
4. Date alignment — keep only 2025 rows (removes GPS resets to 1970/2001)
5. Logical validation — dropoff must be strictly after pickup
6. Deduplication — remove exact duplicate rows
7. Threshold filters — distance (0.1–250 mi), fare ($2.50–$3000), wait time (0–60 min), duration (1–360 min)
8. Feature engineering — add `trip_duration_min`, `wait_time_min`, `pickup_hour`, `pickup_day`, `pickup_month`, `pickup_date`, `company`, `net_revenue`

### Phase 3 — Gold Layer (Aggregations)

Six pre-aggregated analytics tables produced by joining silver data with dimension tables:

| Table | Grain | Key Metrics |
|-------|-------|-------------|
| `rides_per_day` | date × company | total_rides, is_weekday, is_holiday, day_name |
| `hourly_demand` | hour × day_of_week × company | total_rides |
| `provider_summary` | date × company | avg_distance, avg_fare, total_net_revenue, avg_driver_pay, avg_slow_zone_index |
| `top_routes` | pickup × dropoff × company | trip_count, rank (top 20), zone names, boroughs |
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
| `GET /` | Health | API root — lists all available endpoints |
| `GET /health` | Health | MinIO connectivity check |
| `GET /rides-per-day` | Demand | Daily ride counts per company with holiday flags |
| `GET /hourly-demand` | Demand | Trip volume by hour and day of week |
| `GET /provider-summary` | Provider | Daily KPIs per company — fare, distance, revenue |
| `GET /top-routes` | Routes | Top 20 pickup→dropoff corridors per company |
| `GET /fare-stats` | Fares | Fare and tip statistics by hour and month |
| `GET /zone-stats` | Zones | Daily stats per taxi zone with congestion index |
| `GET /wait-time` | Wait Time | Average wait time by company and zone |
| `GET /analysis/peak-hours` | Analysis | Busiest and quietest hours per company |
| `GET /analysis/provider-comparison` | Analysis | Uber vs Lyft vs Via vs Juno comparison |
| `GET /analysis/slow-zones` | Analysis | Most congested zones ranked by slow-zone index |
| `GET /analysis/holiday-impact` | Analysis | Holiday vs weekday demand comparison |
| `GET /analysis/avg-wait-time` | Analysis | Average wait time per company and borough |

**Total: 14 endpoints**

---

## Dashboard

Open `dashboard.html` in a browser while the API is running.

Charts included:
- Summary KPIs (total rides per company, avg fare)
- Rides per day over time — line chart
- Peak hours — bar chart by hour of day
- Holiday impact — weekday vs weekend vs holiday
- Provider comparison — Uber vs Lyft vs Via vs Juno
- Top 10 routes — horizontal bar chart
- Average fare by hour — line chart
- Average wait time by borough — bar chart

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
  -e MINIO_ROOT_PASSWORD=minioadmin123 \
  quay.io/minio/minio server /data --console-address ":9001"
```

MinIO console: http://localhost:9001

### Step 2 — Download and upload raw data

```bash
python Download_raw.py
python Upload_minio.py
python Verifysparkread.py
```

### Step 3 — Run Spark pipelines

```bash
python silverLayer.py
python goldlayer.py
```

### Step 4 — Start the API

```bash
pip install -r requirements.txt
uvicorn API:app --reload --port 8000
```

### Step 5 — Open the dashboard

Open `dashboard.html` in Firefox:
```bash
firefox file:///home/ruwan-fernando/dashboard.html
```

---

## Team

| Name | Role |
|------|------|
| Chamith Priyamal | Data ingestion, Spark silver pipeline, Spark gold pipeline, data lake structure |
| Ruwan Fernando | FastAPI layer, DuckDB integration, data analysis endpoints, dashboard |

---

## Resources

| Resource | Link |
|----------|------|
| Final Presentation | [View slides](https://www.dokie.ai/presentation/share/eZn1ALy6WXDR) |
| API Documentation | http://localhost:8000/docs |
| Dashboard | dashboard.html |
| Dataset Source | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |

---

**Course:** Big Data Engineering (DEAI)  
**Year:** 2026  
**Dataset:** NYC TLC Trip Record Data — https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
