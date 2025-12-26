# Weather Data Ingestion Pipeline

## Overview

This project implements an **end-to-end incremental weather data ingestion pipeline** that retrieves live weather station logs from **https://tylerconlon.com/wx/logs/**, stores them locally using **DuckDB**, and makes the data accessible through a **Flask + Plotly dashboard**.

The project focuses on **data engineering**, including incremental ingestion, deduplication, schema design, and reproducible local execution. The ingestion pipeline ensures correctness and continuity as new data arrives.

The ingestion process is designed for continuously updated source logs and supports both on-demand and configurable periodic execution.

The dashboard provides an interactive interface to inspect the ingested data and explore trends across different time windows.


---

## Key Features

- **Incremental ingestion**
  - Fetches only new data based on the latest timestamp stored per station
  - Prevents duplicate writes using `(station, timestamp)` uniqueness
- **Local, portable storage**
  - Uses DuckDB for fast, file-based analytics with no external dependencies
- **Idempotent pipeline**
  - Safe to re-run without corrupting or duplicating data
- **Automatic schema creation**
  - Database and tables are created on first run
- **Live data retrieval**
  - Pulls weather station logs directly from a remote HTTP server
- **Lightweight inspection dashboard**
  - Flask + Plotly used only to inspect time-series data and basic statistics
- **Optional background updates**
  - Supports periodic auto-refresh via a background thread

---

## Project Structure

```
Weather_Dashboard/
├── main.py          # Complete ingestion pipeline + Flask app
├── templates/       # HTML templates for dashboard rendering
├── static/          # Static assets (Plotly, CSS)
└── README.md
```

No data files, database files, or logs are committed to the repository.  
All state is generated locally at runtime.

---

## How the Pipeline Works

### 1. Data Retrieval

- Weather station logs are fetched from a remote HTTP server
- Logs are organized by date and stored locally per station
- On first run, a **full sync** is performed
- On subsequent runs, only new records are appended based on timestamps

---

### 2. Incremental Loading

- Local log files are parsed as newline-delimited JSON
- Records are deduplicated using the most recent timestamp per station
- Data is merged into DuckDB using a `MERGE` statement

This ensures the pipeline is **idempotent** and safe to re-run.

---

### 3. Storage

- DuckDB stores all ingested data in a local database file
- The schema enforces uniqueness on `(station, dt)`
- No external database or cloud service is required

---

### 4. Querying

- Time-windowed queries are supported:
  - Day
  - Week
  - Month
  - Year
- Lightweight aggregations are applied for readability and performance
- Data is reshaped for visualization

---

### 5. Inspection Dashboard

- Flask serves a simple local dashboard
- Plotly renders time-series plots and basic per-station statistics
- The dashboard is intended for interactive inspection and validation of the ingested data rather than full-scale analytics.

## Live Data Updates and Refresh Behavior

Weather station logs are published continuously on the remote server.  
This pipeline is built to ingest new data incrementally as it becomes available.

- Each station’s most recent timestamp is tracked in the database
- On every update cycle, only records newer than the last ingested timestamp are processed
- Previously ingested data is never reprocessed or duplicated

### Update Modes

The pipeline supports two update modes:

- **Automatic updates**
  - A background thread can periodically fetch and ingest new data
  - The update frequency is configurable in the application entry point

- **Manual updates**
  - A dedicated HTTP endpoint allows triggering ingestion on demand
  - Useful for validation, testing, or controlled refresh cycles

This design allows the pipeline to operate continuously, intermittently, or entirely on demand, depending on the use case.

---

## Running the Project

### Prerequisites

- Python 3.9+
- Internet connection (to fetch live station logs)

---

### Install Dependencies

```bash
pip install duckdb pandas flask requests beautifulsoup4 plotly
```

---

### Run the Application

```bash
python main.py
```

On startup:

- The DuckDB database is created automatically
- Weather station logs are ingested incrementally
- A Flask server starts locally

Access the app at:

```
http://127.0.0.1:5000
```

---

## Manual Database Update

A manual update endpoint is available:

```
http://127.0.0.1:5000/update_database
```

This triggers:

- Incremental log retrieval
- Deduplication
- Database merge

Concurrency is handled using a thread lock to prevent overlapping updates.

---

## Design Notes

- DuckDB was chosen for its simplicity, speed, and zero external dependencies
- Flask and Plotly are intentionally minimal
- The ingestion pipeline is the primary focus, with the dashboard serving as a lightweight inspection interface.
- No data is committed to GitHub to ensure portability and reproducibility

---

## Intended Use

This project demonstrates:

- Incremental ingestion patterns
- Local analytical pipelines
- Practical data engineering workflows
- Reproducible, self-contained systems
- A foundation for more advanced ingestion architectures