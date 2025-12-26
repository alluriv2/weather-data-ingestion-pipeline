import os
import json
import time
import threading
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import Flask, render_template, send_from_directory



# ---------------------------------------------------------
# Setup
# ---------------------------------------------------------

app = Flask(__name__, static_url_path="")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "weather.duckdb")
con = duckdb.connect(DB_PATH, read_only=False)
update_lock = threading.Lock()



# ---------------------------------------------------------
# Helper for data gathering
# ---------------------------------------------------------

def get_last_timestamps_per_station():
    sql = """
        SELECT station, MAX(dt) AS last_ts
        FROM weather
        GROUP BY station;
    """
    rows = con.execute(sql).fetchall()
    station_ts = {station: ts for station, ts in rows}

    return station_ts



# ---------------------------------------------------------
# Download & Update Weather Station Files
# ---------------------------------------------------------

def update_station(folder, url, station_name):
    os.makedirs(folder, exist_ok=True)

    # ---------------------------------------------------
    # 1. READ LAST TIMESTAMP FOR THIS STATION FROM DB
    # ---------------------------------------------------
    station_ts_map = get_last_timestamps_per_station()
    last_ts = station_ts_map.get(station_name, None)

    print("TYPE:", type(last_ts), "VALUE:", last_ts)

    full_sync = (last_ts is None)

    if full_sync:
        print(f"[{station_name}] No previous data: FULL SYNC")
        start_date = "00000000"
    else:
        print(f"[{station_name}] Last updated timestamp = {last_ts}")
        start_date = last_ts.strftime("%Y%m%d")

    # ---------------------------------------------------
    # 2. LIST ALL FILES ON SERVER
    # ---------------------------------------------------
    index = requests.get(url)
    soup = BeautifulSoup(index.text, "html.parser")

    server_files = sorted([
        href for href in (a.get("href") for a in soup.find_all("a"))
        if href and href.endswith(".txt") and href[:8].isdigit()
    ])

    print(f"[{station_name}] Syncing files >= {start_date}")

    # ---------------------------------------------------
    # 3. PROCESS FILES
    # ---------------------------------------------------
    for filename in server_files:
        file_date = filename[:8]  
        file_url = url + filename
        local_path = os.path.join(folder, filename)

        if file_date < start_date:
            continue

        server_text = requests.get(file_url).text
        server_lines = [line.strip() for line in server_text.split("\n") if line.strip()]

        # ---------------------------------------------------
        # CASE A: FULL SYNC OR MISSING LOCAL FILE
        # ---------------------------------------------------
        if full_sync or not os.path.exists(local_path):
            print(f"[{station_name}] Downloading full file: {filename}")
            with open(local_path, "w") as f:
                f.write(server_text)
            continue 

        # ---------------------------------------------------
        # CASE B: INCREMENTAL APPEND
        # ---------------------------------------------------
        print(f"[{station_name}] Updating: {filename}")

        new_lines = []

        for line in server_lines:
            try:
                obj = json.loads(line)
                raw_dt = obj["dt"]

                if len(raw_dt) > 5 and (raw_dt[-5] in ['+', '-']) and raw_dt[-3] != ':':
                    raw_dt = raw_dt[:-2] + ":" + raw_dt[-2:]

                dt = datetime.fromisoformat(raw_dt)
                dt = dt.replace(tzinfo=None)

            except:
                continue  # skip malformed lines silently

            if dt > last_ts:
                new_lines.append(line)

        # Append only new data
        if new_lines:
            print(f"[{station_name}] Appending {len(new_lines)} new lines")
            with open(local_path, "a") as f:
                for nl in new_lines:
                    f.write(nl + "\n")
        else:
            print(f"[{station_name}] No new data for this file")

    print(f"[{station_name}] Station update complete.\n")



# ---------------------------------------------------------
# Weather table creation
# ---------------------------------------------------------

con.execute("""
CREATE TABLE IF NOT EXISTS weather (
    station VARCHAR,
    dt TIMESTAMP,
    vel_avg_mph DOUBLE,
    vel_min_mph DOUBLE,
    vel_max_mph DOUBLE,
    dir_avg_deg INTEGER,
    temps_avg_f DOUBLE,
    temps_min_f DOUBLE,
    temps_max_f DOUBLE,
    tempb_avg_f DOUBLE,
    tempb_min_f DOUBLE,
    tempb_max_f DOUBLE,
    hum_avg_pct DOUBLE,
    hum_min_pct DOUBLE,
    hum_max_pct DOUBLE,
    pres_avg_pa INTEGER,
    pres_min_pa INTEGER,
    pres_max_pa INTEGER,
    lux_avg_lx INTEGER,
    lux_min_lx INTEGER,
    lux_max_lx INTEGER,
    rain_inc_count INTEGER,
    rain_inc_in DOUBLE,
    uptime_seconds INTEGER,
    millis BIGINT,
    node_ip VARCHAR,
    wifi_ssid VARCHAR,
    hostname VARCHAR,
    server_rmt_ip VARCHAR,
    server_svr_dt TIMESTAMP,

    UNIQUE(station, dt)
);
""")



# ---------------------------------------------------------
# DuckDB Incremental Loader
# ---------------------------------------------------------

pattern = os.path.join(BASE_DIR, "wx_*", "**", "*.txt")
pattern = pattern.replace("\\", "/") 

def load_weather_incremental():
    query = f"""
    WITH raw AS (
        SELECT
            json_extract_string(json, '$.node.hostname') AS station,
            TRY_CAST(json_extract_string(json, '$.dt') AS TIMESTAMP) AS dt,

            TRY_CAST(json_extract(json, '$.vel_avg_mph') AS DOUBLE) AS vel_avg_mph,
            TRY_CAST(json_extract(json, '$.vel_min_mph') AS DOUBLE) AS vel_min_mph,
            TRY_CAST(json_extract(json, '$.vel_max_mph') AS DOUBLE) AS vel_max_mph,
            TRY_CAST(json_extract(json, '$.dir_avg_deg') AS INTEGER) AS dir_avg_deg,

            TRY_CAST(json_extract(json, '$.temps_avg_f') AS DOUBLE) AS temps_avg_f,
            TRY_CAST(json_extract(json, '$.temps_min_f') AS DOUBLE) AS temps_min_f,
            TRY_CAST(json_extract(json, '$.temps_max_f') AS DOUBLE) AS temps_max_f,

            TRY_CAST(json_extract(json, '$.tempb_avg_f') AS DOUBLE) AS tempb_avg_f,
            TRY_CAST(json_extract(json, '$.tempb_min_f') AS DOUBLE) AS tempb_min_f,
            TRY_CAST(json_extract(json, '$.tempb_max_f') AS DOUBLE) AS tempb_max_f,

            TRY_CAST(json_extract(json, '$.hum_avg_pct') AS DOUBLE) AS hum_avg_pct,
            TRY_CAST(json_extract(json, '$.hum_min_pct') AS DOUBLE) AS hum_min_pct,
            TRY_CAST(json_extract(json, '$.hum_max_pct') AS DOUBLE) AS hum_max_pct,

            TRY_CAST(json_extract(json, '$.pres_avg_pa') AS INTEGER) AS pres_avg_pa,
            TRY_CAST(json_extract(json, '$.pres_min_pa') AS INTEGER) AS pres_min_pa,
            TRY_CAST(json_extract(json, '$.pres_max_pa') AS INTEGER) AS pres_max_pa,

            TRY_CAST(json_extract(json, '$.lux_avg_lx') AS INTEGER) AS lux_avg_lx,
            TRY_CAST(json_extract(json, '$.lux_min_lx') AS INTEGER) AS lux_min_lx,
            TRY_CAST(json_extract(json, '$.lux_max_lx') AS INTEGER) AS lux_max_lx,

            TRY_CAST(json_extract(json, '$.rain_inc_count') AS INTEGER) AS rain_inc_count,
            TRY_CAST(json_extract(json, '$.rain_inc_in') AS DOUBLE) AS rain_inc_in,

            TRY_CAST(json_extract(json, '$.node.uptime_seconds') AS INTEGER) AS uptime_seconds,
            TRY_CAST(json_extract(json, '$.node.millis') AS BIGINT) AS millis,
            TRY_CAST(json_extract_string(json, '$.node.ip') AS VARCHAR) AS node_ip,
            TRY_CAST(json_extract_string(json, '$.node.wifi_ssid') AS VARCHAR) AS wifi_ssid,

            json_extract_string(json, '$.node.hostname') AS hostname,

            TRY_CAST(json_extract_string(json, '$.server.rmt_ip') AS VARCHAR) AS server_rmt_ip,
            TRY_CAST(json_extract_string(json, '$.server.svr_dt') AS TIMESTAMP) AS server_svr_dt
        FROM read_ndjson_objects('{pattern}')
        WHERE json_valid(json)
    ),

    -- Deduplicate rows (same station + dt)
    dedup AS (
        SELECT *
        FROM (
            SELECT 
                raw.*,
                ROW_NUMBER() OVER (
                    PARTITION BY station, dt
                    ORDER BY millis DESC
                ) AS rn
            FROM raw
        )
        WHERE rn = 1    -- keep only newest row per timestamp
    )

    MERGE INTO weather AS w
    USING dedup AS n
    ON w.station = n.station AND w.dt = n.dt

    WHEN MATCHED THEN UPDATE SET
        vel_avg_mph = n.vel_avg_mph,
        vel_min_mph = n.vel_min_mph,
        vel_max_mph = n.vel_max_mph,
        dir_avg_deg = n.dir_avg_deg,
        temps_avg_f = n.temps_avg_f,
        temps_min_f = n.temps_min_f,
        temps_max_f = n.temps_max_f,
        tempb_avg_f = n.tempb_avg_f,
        tempb_min_f = n.tempb_min_f,
        tempb_max_f = n.tempb_max_f,
        hum_avg_pct = n.hum_avg_pct,
        hum_min_pct = n.hum_min_pct,
        hum_max_pct = n.hum_max_pct,
        pres_avg_pa = n.pres_avg_pa,
        pres_min_pa = n.pres_min_pa,
        pres_max_pa = n.pres_max_pa,
        lux_avg_lx = n.lux_avg_lx,
        lux_min_lx = n.lux_min_lx,
        lux_max_lx = n.lux_max_lx,
        rain_inc_count = n.rain_inc_count,
        rain_inc_in = n.rain_inc_in,
        uptime_seconds = n.uptime_seconds,
        millis = n.millis,
        node_ip = n.node_ip,
        wifi_ssid = n.wifi_ssid,
        hostname = n.hostname,  
        server_rmt_ip = n.server_rmt_ip,
        server_svr_dt = n.server_svr_dt

    WHEN NOT MATCHED THEN INSERT VALUES (
        n.station, n.dt,
        n.vel_avg_mph, n.vel_min_mph, n.vel_max_mph,
        n.dir_avg_deg,
        n.temps_avg_f, n.temps_min_f, n.temps_max_f,
        n.tempb_avg_f, n.tempb_min_f, n.tempb_max_f,
        n.hum_avg_pct, n.hum_min_pct, n.hum_max_pct,
        n.pres_avg_pa, n.pres_min_pa, n.pres_max_pa,
        n.lux_avg_lx, n.lux_min_lx, n.lux_max_lx,
        n.rain_inc_count, n.rain_inc_in,
        n.uptime_seconds, n.millis, n.node_ip, n.wifi_ssid,
        n.hostname, 
        n.server_rmt_ip, n.server_svr_dt
    );
    """
    con.execute(query)
    print("Incremental load complete.")



# ---------------------------------------------------------
# Auto-update the files and the DB
# ---------------------------------------------------------

def perform_database_update():
    wav_folder = os.path.join(BASE_DIR, "wx_waverly")
    east_folder = os.path.join(BASE_DIR, "wx_east_st")

    print("Updating wx_waverly...")
    update_station(
        folder=wav_folder,
        url="https://tylerconlon.com/wx/logs/wx_waverly/",
        station_name="wx_waverly"
    )

    print("Updating wx_east_st...")
    update_station(
        folder=east_folder,
        url="https://tylerconlon.com/wx/logs/wx_east_st/",
        station_name="wx_east_st"
    )

    print("Loading data into DuckDB…")
    load_weather_incremental()

    print("Full update completed.")



def background_updater(interval_minutes=2):
    while True:
        try:
            with update_lock:
                print("\n[Auto Update] Checking for new data…")
                perform_database_update()
                print("[Auto Update] Completed.\n")

        except Exception as e:
            print(f"[Auto Update] Error: {e}")

        time.sleep(interval_minutes * 60)



# ---------------------------------------------------------
# Helper for data queries
# ---------------------------------------------------------

def to_wide_json(df, time_col):
    df = df.copy()

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df[time_col] = df[time_col].dt.strftime("%Y-%m-%dT%H:%M:%S")

    wide = df.pivot_table(
        index=time_col,
        columns="station",
        aggfunc="mean"     
    )

    wide.columns = [f"{station}_{metric}" for (metric, station) in wide.columns]

    wide = wide.reset_index()

    wide = wide.where(pd.notnull(wide), None)

    return wide.to_dict(orient="records")



def filter_metric(json_rows, metric):
    """
    Keep only timestamp + columns that end with f"_{metric}".
    Example: metric="temperature" keeps "wx_east_st_temperature", "wx_waverly_temperature", ...
    """
    suffix = f"_{metric}"
    filtered = []

    for row in json_rows:
        new_row = {"timestamp": row.get("timestamp")}

        for key, value in row.items():
            if key.endswith(suffix):
                new_row[key] = value

        filtered.append(new_row)

    return filtered



def get_latest_timestamp():
    row = con.execute("SELECT MAX(dt) AS latest FROM weather;").fetchone()
    return row[0]



def get_time_window(window):
    """
    window: 'day', 'week', 'month', or 'year'
    returns: (start_ts, end_ts)
    """

    latest = get_latest_timestamp()

    if window == "day":
        start = latest - timedelta(days=1)
    elif window == "week":
        start = latest - timedelta(days=7)
    elif window == "month":
        start = latest - timedelta(days=30)
    elif window == "year":
        start = latest - timedelta(days=365)
    else:
        raise ValueError("Invalid window type")

    return start, latest



# ---------------------------------------------------------
# Dashboard Data Queries
# ---------------------------------------------------------

def get_day_df():
    start, end = get_time_window("day")
    sql = f"""
        SELECT 
            DATE_TRUNC('minute', dt) AS timestamp,
            station,
            AVG(temps_avg_f) AS temperature,
            AVG(vel_avg_mph) AS wind_speed,
            AVG(dir_avg_deg) AS wind_direction,
            AVG(lux_avg_lx) AS lux,
            SUM(rain_inc_in) AS rain_inches
        FROM weather
        WHERE dt BETWEEN '{start}' AND '{end}'
        GROUP BY timestamp, station
        ORDER BY timestamp, station;
    """
    df = con.execute(sql).df()
    return to_wide_json(df, "timestamp")



def get_week_df():
    start, end = get_time_window("week")
    sql = f"""
        SELECT 
            DATE_TRUNC('minute', dt)
                - INTERVAL (EXTRACT(minute FROM dt) % 5) MINUTE AS timestamp,
            station,
            AVG(temps_avg_f) AS temperature,
            AVG(vel_avg_mph) AS wind_speed,
            AVG(dir_avg_deg) AS wind_direction,
            AVG(lux_avg_lx) AS lux,
            SUM(rain_inc_in) AS rain_inches
        FROM weather
        WHERE dt BETWEEN '{start}' AND '{end}'
        GROUP BY timestamp, station
        ORDER BY timestamp, station;
    """
    df = con.execute(sql).df()
    return to_wide_json(df, "timestamp")



def get_month_df():
    start, end = get_time_window("month")
    sql = f"""
        SELECT 
            DATE_TRUNC('minute', dt)
                - INTERVAL (EXTRACT(minute FROM dt) % 15) MINUTE AS timestamp,
            station,
            AVG(temps_avg_f) AS temperature,
            AVG(vel_avg_mph) AS wind_speed,
            AVG(dir_avg_deg) AS wind_direction,
            AVG(lux_avg_lx) AS lux,
            SUM(rain_inc_in) AS rain_inches
        FROM weather
        WHERE dt BETWEEN '{start}' AND '{end}'
        GROUP BY timestamp, station
        ORDER BY timestamp, station;
    """
    df = con.execute(sql).df()
    return to_wide_json(df, "timestamp")



def get_year_df():
    start, end = get_time_window("year")
    sql = f"""
        SELECT 
            DATE_TRUNC('hour', dt) AS timestamp,
            station,
            AVG(temps_avg_f) AS temperature,
            AVG(vel_avg_mph) AS wind_speed,
            AVG(dir_avg_deg) AS wind_direction,
            AVG(lux_avg_lx) AS lux,
            SUM(rain_inc_in) AS rain_inches
        FROM weather
        WHERE dt BETWEEN '{start}' AND '{end}'
        GROUP BY timestamp, station
        ORDER BY timestamp, station;
    """
    df = con.execute(sql).df()
    return to_wide_json(df, "timestamp")



# ---------------------------------------------------------
# Metric Stats for Gauges
# ---------------------------------------------------------

def get_metric_stats_per_station(metric):
    col_map = {
        "temperature": "temps_avg_f",
        "wind_speed": "vel_avg_mph",
        "wind_direction": "dir_avg_deg",
        "lux": "lux_avg_lx",
        "rain_inches": "rain_inc_in"
    }

    if metric not in col_map:
        return None

    col = col_map[metric]

    sql = f"""
        SELECT 
            station,
            MIN({col}) AS min_val,
            MAX({col}) AS max_val
        FROM weather
        WHERE {col} IS NOT NULL
        GROUP BY station
    """

    df = con.execute(sql).df()
    stats = {}

    for _, row in df.iterrows():
        stats[row["station"]] = {
            "min": float(row["min_val"]) if row["min_val"] is not None else None,
            "max": float(row["max_val"]) if row["max_val"] is not None else None,
        }

    return stats



# ---------------------------------------------------------
# Routes
# ---------------------------------------------------------

@app.route("/")
def home():
    return render_template("index.html")



@app.route("/dashboard/<metric>/<range>")
def dashboard_range_metric(metric, range):

    if range == "day":
        data = get_day_df()
    elif range == "week":
        data = get_week_df()
    elif range == "month":
        data = get_month_df()
    elif range == "year":
        data = get_year_df()
    else:
        return "Invalid range", 400

    filtered = filter_metric(data, metric)
    station_stats = get_metric_stats_per_station(metric)
    print("STATS SENT TO TEMPLATE:", station_stats)

    return render_template(
        "timeseries.html",
        data=filtered,
        title=f"{range.capitalize()} — {metric.replace('_',' ').title()}",
        metric=metric,
        station_stats=station_stats
    )



@app.route("/update_database")
def update_database():
    acquired = False
    try:
        acquired = update_lock.acquire(timeout=1)

        if not acquired:
            return "Update already running, please wait...", 429

        wav = os.path.join(BASE_DIR, "wx_waverly")
        east = os.path.join(BASE_DIR, "wx_east_st")

        print("Updating station logs for wx_waverly")
        update_station(
            folder=wav,
            url="https://tylerconlon.com/wx/logs/wx_waverly/",
            station_name="wx_waverly"
        )

        print("Updating station logs for wx_east_st")
        update_station(
            folder=east,
            url="https://tylerconlon.com/wx/logs/wx_east_st/",
            station_name="wx_east_st"
        )

        print("Loading data into DuckDB")
        load_weather_incremental()
        print("[Manual Update] Completed.")

        return "Database manual update complete."

    except Exception as e:
        return f"Error during database update: {e}", 500

    finally:
        if acquired:
            update_lock.release()



if __name__ == "__main__":
    updater = threading.Thread(
        target=background_updater,
        args=(2,),   
        daemon=True
    )
    updater.start()

    app.run(
        host=os.getenv('HOSTIP', '127.0.0.1'),
        debug=os.getenv('FLASKDEBUG', True),
        port=os.getenv('PORT', '5000'),
        use_reloader=False
    )
    
