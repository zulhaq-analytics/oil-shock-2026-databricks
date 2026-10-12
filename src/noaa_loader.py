"""NOAA National Hurricane Center: active storm snapshots and HURDAT2 Atlantic history."""
import re
import json
import datetime as dt

from pyspark.sql import types as T
from src import oil_http as oh

CURRENT_URL = "https://www.nhc.noaa.gov/CurrentStorms.json"
HURDAT_DIR = "https://www.nhc.noaa.gov/data/hurdat/"
ACTIVE_TABLE = f"{oh.CATALOG}.bronze.noaa_active_storms"
TRACK_TABLE = f"{oh.CATALOG}.bronze.noaa_hurdat2"
FIRST_YEAR = 1990

ACTIVE_SCHEMA = T.StructType([
    T.StructField("snapshot_at", T.TimestampType()),
    T.StructField("storm_id", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("classification", T.StringType()),
    T.StructField("intensity_kt", T.DoubleType()),
    T.StructField("pressure_mb", T.DoubleType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("last_update", T.StringType()),
    T.StructField("storm_json", T.StringType()),
])
TRACK_SCHEMA = T.StructType([
    T.StructField("storm_id", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("obs_time", T.StringType()),
    T.StructField("record_id", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("max_wind_kt", T.DoubleType()),
    T.StructField("min_pressure_mb", T.DoubleType()),
    T.StructField("source_file", T.StringType()),
    T.StructField("loaded_at", T.TimestampType()),
])


def ensure_tables():
    s = oh.spark()
    s.sql(f"""CREATE TABLE IF NOT EXISTS {ACTIVE_TABLE} (
        snapshot_at TIMESTAMP, storm_id STRING, name STRING, classification STRING,
        intensity_kt DOUBLE, pressure_mb DOUBLE, latitude DOUBLE, longitude DOUBLE,
        last_update STRING, storm_json STRING)
        COMMENT 'Snapshots of NOAA NHC active storms (one row per storm per run)'""")
    s.sql(f"""CREATE TABLE IF NOT EXISTS {TRACK_TABLE} (
        storm_id STRING, name STRING, obs_time STRING, record_id STRING, status STRING,
        latitude DOUBLE, longitude DOUBLE, max_wind_kt DOUBLE, min_pressure_mb DOUBLE,
        source_file STRING, loaded_at TIMESTAMP)
        COMMENT 'NOAA HURDAT2 Atlantic best-track history (6-hourly), 1990 onward'""")


def _num(v):
    try:
        x = float(v)
        return None if x in (-999.0, -99.0) else x
    except (TypeError, ValueError):
        return None


def _coord(text):
    text = (text or "").strip()
    if not text:
        return None
    value = _num(text[:-1])
    if value is None:
        return None
    return -value if text[-1] in "SW" else value


def run_active():
    ensure_tables()
    dl = oh.Downloader("noaa", timeout=60)
    try:
        storms = dl.get("current_storms", CURRENT_URL).json().get("activeStorms", [])
        dl.note_rows(len(storms), outcome="ok" if storms else "no_data")
    finally:
        dl.flush()
    if not storms:
        return {"active_storms": 0}
    now = dt.datetime.utcnow()
    records = [{
        "snapshot_at": now, "storm_id": s.get("id"), "name": s.get("name"),
        "classification": s.get("classification"),
        "intensity_kt": _num(s.get("intensity")), "pressure_mb": _num(s.get("pressure")),
        "latitude": s.get("latitudeNumeric") if s.get("latitudeNumeric") is not None else _coord(s.get("latitude")),
        "longitude": s.get("longitudeNumeric") if s.get("longitudeNumeric") is not None else _coord(s.get("longitude")),
        "last_update": s.get("lastUpdate"), "storm_json": json.dumps(s),
    } for s in storms]
    oh.spark().createDataFrame(records, ACTIVE_SCHEMA).write.mode("append").saveAsTable(ACTIVE_TABLE)
    oh.set_watermark("noaa", "active_storms", now.isoformat(timespec="seconds"), len(records))
    return {"active_storms": len(records), "names": [r["name"] for r in records]}


def run_hurdat():
    ensure_tables()
    dl = oh.Downloader("noaa", timeout=120)
    try:
        listing = dl.get("hurdat_dir", HURDAT_DIR).text
        files = sorted(set(re.findall(r"hurdat2-1851-[\w\-]+\.txt", listing)))
        if not files:
            raise RuntimeError("No HURDAT2 Atlantic file found")
        latest = files[-1]
        if oh.file_already_loaded("noaa", latest):
            return {"file": latest, "status": "skipped (already loaded)"}
        text = dl.get("hurdat_file", HURDAT_DIR + latest).text
    finally:
        dl.flush()

    loaded_at = dt.datetime.utcnow()
    records, storm_id, name = [], None, None
    for line in text.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 3 and re.match(r"^[A-Z]{2}\d{6}$", parts[0]):
            storm_id, name = parts[0], parts[1]
            continue
        if storm_id is None or len(parts) < 8 or not parts[0].isdigit():
            continue
        if int(parts[0][:4]) < FIRST_YEAR:
            continue
        d, t = parts[0], parts[1].zfill(4)
        records.append({
            "storm_id": storm_id, "name": name,
            "obs_time": f"{d[:4]}-{d[4:6]}-{d[6:8]}T{t[:2]}:{t[2:]}:00Z",
            "record_id": parts[2] or None, "status": parts[3],
            "latitude": _coord(parts[4]), "longitude": _coord(parts[5]),
            "max_wind_kt": _num(parts[6]), "min_pressure_mb": _num(parts[7]),
            "source_file": latest, "loaded_at": loaded_at,
        })

    s = oh.spark()
    s.sql(f"DELETE FROM {TRACK_TABLE}")
    s.createDataFrame(records, TRACK_SCHEMA).write.mode("append").saveAsTable(TRACK_TABLE)
    oh.log_file("noaa", latest, HURDAT_DIR + latest, len(text), len(records), "loaded")
    oh.set_watermark("noaa", "hurdat2", max(r["obs_time"] for r in records)[:10], len(records))
    return {"file": latest, "rows": len(records),
            "storms": len({r["storm_id"] for r in records}),
            "first": min(r["obs_time"] for r in records)[:10],
            "last": max(r["obs_time"] for r in records)[:10]}
