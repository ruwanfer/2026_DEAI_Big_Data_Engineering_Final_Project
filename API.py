NYC HVFHV Taxi Analytics API
FastAPI + DuckDB over MinIO gold layer
Dataset : High Volume For-Hire Vehicle (Uber, Lyft, Via, Juno)
Period  : January - March 2025
Bucket  : nyc-taxi

Run:
    pip install fastapi uvicorn duckdb
    uvicorn main:app --reload --port 8000

Docs:
    http://localhost:8000/docs
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import os

app = FastAPI(
    title="NYC HVFHV Taxi Analytics API",
    description=(
        "Analytics REST API for NYC High Volume For-Hire Vehicle trip data (Jan-Mar 2025). "
        "Covers Uber (HV0003), Lyft (HV0005), Via (HV0004) and Juno (HV0002). "
        "Powered by DuckDB reading Parquet files from MinIO gold layer."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",    "localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY",   "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY",   "minioadmin123")
BUCKET         = os.getenv("MINIO_BUCKET",       "nyc-taxi")


def get_con():
    con = duckdb.connect()
    con.execute(f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint          = '{MINIO_ENDPOINT}';
        SET s3_access_key_id     = '{MINIO_ACCESS}';
        SET s3_secret_access_key = '{MINIO_SECRET}';
        SET s3_use_ssl           = false;
        SET s3_url_style         = 'path';
    """)
    return con


def gold(table: str) -> str:
    return f"s3://{BUCKET}/gold/{table}/**/*.parquet"


# Health 

@app.get("/", tags=["Health"])
def root():
    return {
        "status": "ok",
        "api":    "NYC HVFHV Taxi Analytics",
        "docs":   "/docs",
        "bucket": BUCKET,
    }


@app.get("/health", tags=["Health"])
def health():
    try:
        con = get_con()
        n = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{gold('rides_per_day')}')"
        ).fetchone()[0]
        return {"status": "healthy", "minio": "connected", "rides_per_day_rows": n}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"MinIO unreachable: {e}")


# Core endpoints 

@app.get("/rides-per-day", tags=["Demand"], summary="Daily ride counts per company")
def rides_per_day(
    company:    str = Query(None, description="Uber, Lyft, Via or Juno"),
    month_name: str = Query(None, description="January, February or March"),
    limit:      int = Query(200, ge=1, le=1000),
):
    con = get_con()
    where = []
    if company:    where.append(f"company = '{company}'")
    if month_name: where.append(f"month_name = '{month_name}'")
    clause = "WHERE " + " AND ".join(where) if where else ""
    try:
        rows = con.execute(f"""
            SELECT pickup_date, company, hvfhs_license_num, total_rides,
                   is_weekday, is_holiday, day_name, month_name
            FROM read_parquet('{gold("rides_per_day")}')
            {clause}
            ORDER BY pickup_date, company
            LIMIT {limit}
        """).fetchdf()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"count": len(rows), "data": rows.to_dict(orient="records")}


@app.get("/provider-summary", tags=["Provider"], summary="Daily KPIs per company")
def provider_summary(
    company: str = Query(None, description="Uber, Lyft, Via or Juno"),
    limit:   int = Query(200, ge=1, le=1000),
):
    con = get_con()
    where = f"WHERE company = '{company}'" if company else ""
    try:
        rows = con.execute(f"""
            SELECT pickup_date, company, hvfhs_license_num, total_rides,
                   ROUND(avg_distance_miles, 2)  AS avg_distance_miles,
                   ROUND(avg_duration_min, 2)    AS avg_duration_min,
                   ROUND(avg_fare, 2)            AS avg_fare,
                   ROUND(avg_driver_pay, 2)      AS avg_driver_pay,
                   ROUND(total_net_revenue, 2)   AS total_net_revenue,
                   ROUND(avg_slow_zone_index, 4) AS avg_slow_zone_index,
                   is_weekday, is_holiday, day_name
            FROM read_parquet('{gold("provider_summary")}')
            {where}
            ORDER BY pickup_date, company
            LIMIT {limit}
        """).fetchdf()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"count": len(rows), "data": rows.to_dict(orient="records")}


@app.get("/top-routes", tags=["Routes"], summary="Top 20 pickup to dropoff corridors per company")
def top_routes(
    company:      str = Query(None, description="Uber, Lyft, Via or Juno"),
    pickup_zone:  str = Query(None, description="Partial match on pickup zone name"),
    dropoff_zone: str = Query(None, description="Partial match on dropoff zone name"),
    limit:        int = Query(20, ge=1, le=100),
):
    con = get_con()
    where = []
    if company:      where.append(f"company = '{company}'")
    if pickup_zone:  where.append(f"LOWER(pickup_zone) LIKE '%{pickup_zone.lower()}%'")
    if dropoff_zone: where.append(f"LOWER(dropoff_zone) LIKE '%{dropoff_zone.lower()}%'")
    clause = "WHERE " + " AND ".join(where) if where else ""
    try:
        rows = con.execute(f"""
            SELECT company, hvfhs_license_num, pickup_zone, pickup_borough,
                   dropoff_zone, dropoff_borough, trip_count, rank
            FROM read_parquet('{gold("top_routes")}')
            {clause}
            ORDER BY company, rank
            LIMIT {limit}
        """).fetchdf()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"count": len(rows), "data": rows.to_dict(orient="records")}


@app.get("/hourly-demand", tags=["Demand"], summary="Trip volume by hour and day of week")
def hourly_demand(
    company:    str = Query(None, description="Uber, Lyft, Via or Juno"),
    hour:       int = Query(None, description="Hour 0-23"),
    pickup_day: int = Query(None, description="1=Sunday to 7=Saturday"),
):
    con = get_con()
    where = []
    if company:              where.append(f"company = '{company}'")
    if hour is not None:     where.append(f"pickup_hour = {hour}")
    if pickup_day is not None: where.append(f"pickup_day = {pickup_day}")
    clause = "WHERE " + " AND ".join(where) if where else ""
    try:
        rows = con.execute(f"""
            SELECT pickup_hour, pickup_day, company, hvfhs_license_num, total_rides
            FROM read_parquet('{gold("hourly_demand")}')
            {clause}
            ORDER BY company, pickup_day, pickup_hour
        """).fetchdf()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"count": len(rows), "data": rows.to_dict(orient="records")}


@app.get("/fare-stats", tags=["Fares"], summary="Fare and tip breakdown by hour and month")
def fare_stats(
    company: str = Query(None, description="Uber, Lyft, Via or Juno"),
    hour:    int = Query(None, description="Hour 0-23"),
    month:   int = Query(None, description="Month number 1-3"),
):
    con = get_con()
    where = []
    if company:          where.append(f"company = '{company}'")
    if hour is not None: where.append(f"pickup_hour = {hour}")
    if month:            where.append(f"pickup_month = {month}")
    clause = "WHERE " + " AND ".join(where) if where else ""
    try:
        rows = con.execute(f"""
            SELECT pickup_hour, pickup_month, company, hvfhs_license_num, trip_count,
                   ROUND(avg_fare, 2)     AS avg_fare,
                   ROUND(min_fare, 2)     AS min_fare,
                   ROUND(max_fare, 2)     AS max_fare,
                   ROUND(avg_tip, 2)      AS avg_tip,
                   ROUND(avg_tip_pct, 4)  AS avg_tip_pct,
                   ROUND(avg_driver_pay, 2) AS avg_driver_pay
            FROM read_parquet('{gold("fare_stats")}')
            {clause}
            ORDER BY company, pickup_month, pickup_hour
        """).fetchdf()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"count": len(rows), "data": rows.to_dict(orient="records")}


@app.get("/zone-stats", tags=["Zones"], summary="Daily trip stats per taxi zone")
def zone_stats(
    company: str = Query(None, description="Uber, Lyft, Via or Juno"),
    borough: str = Query(None, description="Manhattan, Brooklyn, Queens, Bronx, Staten Island"),
    limit:   int = Query(100, ge=1, le=1000),
):
    con = get_con()
    where = []
    if company: where.append(f"company = '{company}'")
    if borough: where.append(f"LOWER(borough) = '{borough.lower()}'")
    clause = "WHERE " + " AND ".join(where) if where else ""
    try:
        rows = con.execute(f"""
            SELECT pickup_date, company, hvfhs_license_num, zone_name, borough,
                   service_zone, trip_count,
                   ROUND(avg_fare, 2)            AS avg_fare,
                   ROUND(total_net_revenue, 2)   AS total_net_revenue,
                   ROUND(avg_slow_zone_index, 4) AS avg_slow_zone_index,
                   is_weekday, is_holiday, day_name
            FROM read_parquet('{gold("daily_zone_stats")}')
            {clause}
            ORDER BY trip_count DESC
            LIMIT {limit}
        """).fetchdf()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"count": len(rows), "data": rows.to_dict(orient="records")}


@app.get("/wait-time", tags=["Wait Time"], summary="Average wait time by company and zone")
def wait_time(
    company: str = Query(None, description="Uber, Lyft, Via or Juno"),
    borough: str = Query(None, description="Manhattan, Brooklyn, Queens, Bronx, Staten Island"),
    month:   int = Query(None, description="Month number 1-3"),
    limit:   int = Query(100, ge=1, le=500),
):
    con = get_con()
    where = []
    if company:          where.append(f"company = '{company}'")
    if borough:          where.append(f"LOWER(borough) = '{borough.lower()}'")
    if month is not None: where.append(f"pickup_month = {month}")
    clause = "WHERE " + " AND ".join(where) if where else ""
    try:
        rows = con.execute(f"""
            SELECT company, hvfhs_license_num, zone_name, borough,
                   pickup_hour, pickup_month,
                   ROUND(avg_wait_min, 2) AS avg_wait_min,
                   ROUND(min_wait_min, 2) AS min_wait_min,
                   ROUND(max_wait_min, 2) AS max_wait_min,
                   trip_count
            FROM read_parquet('{gold("wait_time_stats")}')
            {clause}
            ORDER BY avg_wait_min DESC
            LIMIT {limit}
        """).fetchdf()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"count": len(rows), "data": rows.to_dict(orient="records")}


# Analysis endpoints 

@app.get("/analysis/peak-hours", tags=["Analysis"], summary="Q1: Which hours have highest demand?")
def analysis_peak_hours(
    company: str = Query(None, description="Uber, Lyft, Via or Juno"),
):
    con = get_con()
    where = f"WHERE company = '{company}'" if company else ""
    try:
        rows = con.execute(f"""
            SELECT pickup_hour, company, hvfhs_license_num,
                   SUM(total_rides) AS total_rides,
                   RANK() OVER (
                       PARTITION BY company
                       ORDER BY SUM(total_rides) DESC
                   ) AS demand_rank
            FROM read_parquet('{gold("hourly_demand")}')
            {where}
            GROUP BY pickup_hour, company, hvfhs_license_num
            ORDER BY company, demand_rank
        """).fetchdf()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    result = {}
    for co in rows["company"].unique():
        sub = rows[rows["company"] == co].reset_index(drop=True)
        result[co] = {
            "peak_hours_top5":  sub.head(5).to_dict(orient="records"),
            "quiet_hours_top5": sub.tail(5).to_dict(orient="records"),
        }
    return {
        "question": "Which hours have the highest trip demand?",
        "insight":  "Rush hours (7-9 AM and 5-8 PM) dominate. Late night (2-5 AM) is quietest.",
        "data":     result,
    }


@app.get("/analysis/provider-comparison", tags=["Analysis"],
         summary="Q2: Uber vs Lyft vs Via vs Juno comparison")
def analysis_provider_comparison():
    con = get_con()
    try:
        rows = con.execute(f"""
            SELECT company, hvfhs_license_num,
                   COUNT(*)                          AS days_observed,
                   SUM(total_rides)                  AS total_rides,
                   ROUND(AVG(avg_fare), 2)           AS overall_avg_fare,
                   ROUND(AVG(avg_distance_miles), 2) AS overall_avg_distance_miles,
                   ROUND(AVG(avg_duration_min), 2)   AS overall_avg_duration_min,
                   ROUND(AVG(avg_driver_pay), 2)     AS avg_driver_pay,
                   ROUND(SUM(total_net_revenue), 2)  AS total_net_revenue,
                   ROUND(AVG(avg_slow_zone_index), 4) AS avg_slow_zone_index
            FROM read_parquet('{gold("provider_summary")}')
            GROUP BY company, hvfhs_license_num
            ORDER BY total_rides DESC
        """).fetchdf()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {
        "question": "How do Uber, Lyft, Via and Juno compare?",
        "insight":  "Uber dominates volume. Compare avg_fare, driver_pay and net_revenue across providers.",
        "data":     rows.to_dict(orient="records"),
    }


@app.get("/analysis/slow-zones", tags=["Analysis"], summary="Q3: Most congested zones")
def analysis_slow_zones(
    top_n:   int = Query(10, ge=1, le=50),
    borough: str = Query(None, description="Manhattan, Brooklyn, Queens, Bronx, Staten Island"),
):
    con = get_con()
    where = f"WHERE LOWER(borough) = '{borough.lower()}'" if borough else ""
    try:
        rows = con.execute(f"""
            SELECT zone_name, borough,
                   ROUND(AVG(avg_slow_zone_index), 4) AS avg_slow_zone_index,
                   SUM(trip_count)                    AS total_trips,
                   ROUND(SUM(total_net_revenue), 2)   AS total_net_revenue
            FROM read_parquet('{gold("daily_zone_stats")}')
            {where}
            GROUP BY zone_name, borough
            ORDER BY avg_slow_zone_index DESC
            LIMIT {top_n}
        """).fetchdf()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {
        "question": "Which zones are the most congested?",
        "insight":  "Midtown Manhattan and Times Square typically show the highest slow-zone index.",
        "data":     rows.to_dict(orient="records"),
    }


@app.get("/analysis/holiday-impact", tags=["Analysis"], summary="Q4: Holiday impact on demand")
def analysis_holiday_impact():
    con = get_con()
    try:
        rows = con.execute(f"""
            SELECT company, hvfhs_license_num,
                   CASE
                       WHEN is_holiday = true THEN 'public_holiday'
                       WHEN is_weekday = true THEN 'weekday'
                       ELSE 'weekend'
                   END AS day_type,
                   ROUND(AVG(total_rides), 0) AS avg_rides,
                   COUNT(*)                   AS days_count
            FROM read_parquet('{gold("rides_per_day")}')
            GROUP BY company, hvfhs_license_num, day_type
            ORDER BY company, avg_rides DESC
        """).fetchdf()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {
        "question": "How do public holidays affect ride demand?",
        "insight":  "Weekdays have highest demand. Holidays see significant drops across all providers.",
        "data":     rows.to_dict(orient="records"),
    }


@app.get("/analysis/avg-wait-time", tags=["Analysis"],
         summary="Q5: Average wait time by company and borough")
def analysis_avg_wait_time():
    con = get_con()
    try:
        rows = con.execute(f"""
            SELECT company, hvfhs_license_num, borough,
                   ROUND(AVG(avg_wait_min), 2) AS avg_wait_min,
                   ROUND(MIN(min_wait_min), 2) AS best_wait_min,
                   ROUND(MAX(max_wait_min), 2) AS worst_wait_min,
                   SUM(trip_count)             AS total_trips
            FROM read_parquet('{gold("wait_time_stats")}')
            GROUP BY company, hvfhs_license_num, borough
            ORDER BY company, avg_wait_min
        """).fetchdf()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {
        "question": "What is the average wait time per company and borough?",
        "insight":  "Manhattan has shortest waits. Outer boroughs show longer and more variable wait times.",
        "data":     rows.to_dict(orient="records"),
    }
