"""NOAA National Hurricane Center: active storm snapshots, current-season best tracks (b-decks)
and HURDAT2 Atlantic history."""
import re
import json
import datetime as dt

from pyspark.sql import types as T
from src import oil_http as oh

CURRENT_URL = "https://www.nhc.noaa.gov/CurrentStorms.json"
HURDAT_DIR = "https://www.nhc.noaa.gov/data/hurdat/"
BDECK_DIR = "https://ftp.nhc.noaa.gov/atcf/btk/"
ACTIVE_TABLE = f"{oh.CATALOG}.bronze.noaa_active_storms"
TRACK_TABLE = f"{oh.CATALOG}.bronze.noaa_hurdat2"
BDECK_TABLE = f"{oh.CATALOG}.bronze.noaa_bdeck"
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
    for table, note in [(TRACK_TABLE, "NOAA HURDAT2 Atlantic best-track history (6-hourly), 1990 onward"),
                        (BDECK_TABLE, "NOAA NHC current-season Atlantic best tracks (b-decks), refreshed each run")]:
        s.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
            storm_id STRING, name STRING, obs_time STRING, record_id STRING, status STRING,
            latitude DOUBLE, longitude DOUBLE, max_wind_kt DOUBLE, min_pressure_mb DOUBLE,
            source_file STRING, loaded_at TIMESTAMP)
            COMMENT '{note}'""")


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


def _bdeck_coord(text):
    """b-deck coordinates are in tenths of a degree, e.g. 220N -> 22.0, 965W -> -96.5."""
    text = (text or "").strip()
    if len(text) < 2:
        return None
    value = _num(text[:-1])
    if value is None:
        return None
    value = value / 10.0
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


def run_bdecks(year=None):
    """Reload all Atlantic best-track files for the current season (replaces that season's rows)."""
    ensure_tables()
    year = year or dt.date.today().year
    dl = oh.Downloader("noaa", timeout=60)
    records = []
    loaded_at = dt.datetime.utcnow()
    try:
        listing = dl.get("bdeck_dir", BDECK_DIR).text
        files = sorted(set(re.findall(rf"bal\d{{2}}{year}\.dat", listing)))
        for name in files:
            text = dl.get("bdeck_file", BDECK_DIR + name).text
            rows, seen, storm_name = [], set(), None
            for line in text.splitlines():
                p = [x.strip() for x in line.split(",")]
                if len(p) < 11 or p[4] != "BEST":
                    continue
                key = p[2]
                if len(p) > 27 and p[27]:
                    storm_name = p[27]
                if key in seen:
                    continue
                seen.add(key)
                t = p[2]
                rows.append({
                    "storm_id": f"AL{p[1]}{year}", "name": None,
                    "obs_time": f"{t[:4]}-{t[4:6]}-{t[6:8]}T{t[8:10]}:00:00Z",
                    "record_id": None, "status": p[10],
                    "latitude": _bdeck_coord(p[6]), "longitude": _bdeck_coord(p[7]),
                    "max_wind_kt": _num(p[8]), "min_pressure_mb": _num(p[9]) or None,
                    "source_file": name, "loaded_at": loaded_at,
                })
            for r in rows:
                r["name"] = storm_name
            records.extend(rows)
            dl.note_rows(len(rows))
    finally:
        dl.flush()

    s = oh.spark()
    s.sql(f"DELETE FROM {BDECK_TABLE} WHERE storm_id LIKE :p", args={"p": f"AL__{year}"})
    if records:
        s.createDataFrame(records, TRACK_SCHEMA).write.mode("append").saveAsTable(BDECK_TABLE)
        oh.set_watermark("noaa", "bdeck", max(r["obs_time"] for r in records)[:10], len(records))
    return {"season": year, "files": len(files), "rows": len(records),
            "storms": sorted({(r["storm_id"], r["name"]) for r in records})}


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
