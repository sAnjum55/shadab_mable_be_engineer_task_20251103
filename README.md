# 🚀 Mable Backend Engineer Technical Task

This repository contains the implementation for Mable’s backend engineering technical task.

It builds, deploys, benchmarks, and visualizes a Go-based data processing pipeline using ClickHouse and Grafana.

---

## ⚙️ Prerequisites

- Docker and Docker Compose installed  
- Ports available:  
  - `9000` (ClickHouse TCP)  
  - `8123` (ClickHouse HTTP)  
  - `3000` (Grafana UI)

---

## 🧩 Setup and Run

### 1️⃣ Build the project

```bash
docker compose build --no-cache
```

### 2️⃣ Start all services

```bash
docker compose up
```

This starts:
- **ClickHouse** database  
- **Grafana** dashboard  
- **Pipeline Runner** (runs Go benchmarks and pushes metrics)

---

## 🧠 What Happens

1. `main.go` executes benchmark pipelines:
   - `BenchmarkPipeline`
   - `BenchmarkTestStructPipeline`
   - `BenchmarkMableEventPipeline`
2. Each benchmark emits stage metrics asynchronously.
3. Metrics are written to ClickHouse table `mable.pipeline_metrics`.
4. Grafana visualizes pipeline performance.

---

## 📊 View Metrics in Grafana

### Open Grafana
Visit [http://localhost:3000/public-dashboards/a9c65d60162e4b049f02cdcfd45bb8c2](http://localhost:3000/public-dashboards/a9c65d60162e4b049f02cdcfd45bb8c2)

**Login:**
```
User: admin
Pass: admin
```

### Add ClickHouse Data Source
```
URL: http://clickhouse:8123
Database: mable
User: default
Password: mypass
```

Click **Save & Test** — it should say ✅ “Database connection OK”.

---

## 📈 Example Queries

**Stage Duration**
```sql
SELECT stage_name, avg(duration_ms) AS avg_duration
FROM mable.pipeline_metrics
GROUP BY stage_name
ORDER BY avg_duration DESC
```

**Worker Load**
```sql
SELECT worker_id, count() AS total_batches
FROM mable.pipeline_metrics
GROUP BY worker_id
ORDER BY worker_id
```

**Throughput**
```sql
SELECT toStartOfMinute(timestamp) AS minute, sum(output_count) AS total_output
FROM mable.pipeline_metrics
GROUP BY minute
ORDER BY minute ASC
```

---

## 🧪 Optional: Run Benchmarks Locally

```bash
go run main.go -input=pipeline/testdata/mable_event.json -output=benchmarks.csv
```

Results will be saved to `benchmarks.csv`.

---

## 🧹 Stop and Clean Up

```bash
docker compose down -v
```

---

**Author:** Shadab Anjum  
_Mable Backend Engineer Technical Task — November 2025_
