"""Macro drivers: FRED series (with metadata) and OECD composite leading indicators."""
import csv
import io
import json
import datetime as dt

from pyspark.sql import types as T
from src import oil_http as oh

FRED_BASE = "https://api.stlouisfed.org/fred"
FRED_SERIES = {
    "DTWEXBGS": "US dollar, broad trade-weighted index (daily)",
    "DFF": "Fed funds effective rate (daily)",
    "DGS10": "10-year Treasury yield (daily)",
    "CPIAUCSL": "US consumer price index (monthly)",
    "INDPRO": "US industrial production (monthly)",
    "VIXCLS": "VIX market fear index (daily)",
    "POILDUBUSDM": "Dubai crude price, IMF (monthly)",
    "POILBREUSDM": "Brent crude price, IMF (monthly)",
    "POILWTIUSDM": "WTI crude price, IMF (monthly)",
}
FRED_LOOKBACK_DAYS = 400
FRED_OBS = f"{oh.CATALOG}.bronze.fred_observations"
FRED_META = f"{oh.CATALOG}.bronze.fred_series_meta"

OECD_URL = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,/.M.LI...AA...H"
OECD_TABLE = f"{oh.CATALOG}.bronze.oecd_cli"

OBS_SCHEMA = T.StructType([
    T.StructField("series_id", T.StringType()),
    T.StructField("date", T.StringType()),
    T.StructField("value", T.DoubleType()),
    T.StructField("value_raw", T.StringType()),
    T.StructField("loaded_at", T.TimestampType()),
])
META_SCHEMA = T.StructType([
    T.StructField("series_id", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("units", T.StringType()),
    T.StructField("frequency", T.StringType()),
    T.StructField("seasonal_adjustment", T.StringType()),
    T.StructField("last_updated", T.StringType()),
    T.StructField("notes", T.StringType()),
    T.StructField("has_copyright_note", T.BooleanType()),
    T.StructField("loaded_at", T.TimestampType()),
])
OECD_SCHEMA = T.StructType([
    T.StructField("ref_area", T.StringType()),
    T.StructField("area_name", T.StringType()),
    T.StructField("time_period", T.StringType()),
    T.StructField("obs_value", T.DoubleType()),
    T.StructField("row_json", T.StringType()),
    T.StructField("loaded_at", T.TimestampType()),
])


def ensure_tables():
    s = oh.spark()
    s.sql(f"""CREATE TABLE IF NOT EXISTS {FRED_OBS} (
        series_id STRING, date STRING, value DOUBLE, value_raw STRING, loaded_at TIMESTAMP)
        COMMENT 'Raw FRED observations'""")
    s.sql(f"""CREATE TABLE IF NOT EXISTS {FRED_META} (
        series_id STRING, title STRING, units STRING, frequency STRING,
        seasonal_adjustment STRING, last_updated STRING, notes STRING,
        has_copyright_note BOOLEAN, loaded_at TIMESTAMP)
        COMMENT 'FRED series descriptions incl. copyright notes'""")
    s.sql(f"""CREATE TABLE IF NOT EXISTS {OECD_TABLE} (
        ref_area STRING, area_name STRING, time_period STRING, obs_value DOUBLE,
        row_json STRING, loaded_at TIMESTAMP)
        COMMENT 'Raw OECD composite leading indicators (amplitude adjusted)'""")


def _float(v):
    try:
        return float(v) if v not in (None, "", ".") else None
    except (TypeError, ValueError):
        return None


def run_fred(key, mode="recent"):
    oh.register_secret(key)
    ensure_tables()
    s = oh.spark()
    dl = oh.Downloader("fred", timeout=90)
    loaded_at = dt.datetime.utcnow()
    obs_records, meta_records, summary = [], [], []
    start = None if mode == "backfill" else (dt.date.today() - dt.timedelta(days=FRED_LOOKBACK_DAYS)).isoformat()
    try:
        for sid, label in FRED_SERIES.items():
            m = dl.get("series_meta", f"{FRED_BASE}/series",
                       params={"series_id": sid, "api_key": key, "file_type": "json"}).json()
            info = (m.get("seriess") or [{}])[0]
            notes = info.get("notes", "") or ""
            meta_records.append({
                "series_id": sid, "title": info.get("title"), "units": info.get("units"),
                "frequency": info.get("frequency"), "seasonal_adjustment": info.get("seasonal_adjustment"),
                "last_updated": info.get("last_updated"), "notes": notes,
                "has_copyright_note": "copyright" in notes.lower(), "loaded_at": loaded_at})

            params = {"series_id": sid, "api_key": key, "file_type": "json", "limit": 100000}
            if start:
                params["observation_start"] = start
            o = dl.get("observations", f"{FRED_BASE}/series/observations", params=params).json()
            rows = o.get("observations", [])
            dl.note_rows(len(rows), outcome="ok" if rows else "no_data")
            for x in rows:
                obs_records.append({"series_id": sid, "date": x.get("date"),
                                    "value": _float(x.get("value")), "value_raw": x.get("value"),
                                    "loaded_at": loaded_at})
            summary.append((sid, len(rows), rows[0]["date"] if rows else "",
                            rows[-1]["date"] if rows else "", "copyright note" if "copyright" in notes.lower() else ""))
    finally:
        dl.flush()

    s.createDataFrame(meta_records, META_SCHEMA).createOrReplaceTempView("fred_meta_batch")
    s.sql(f"""MERGE INTO {FRED_META} t USING fred_meta_batch u ON t.series_id = u.series_id
              WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    if obs_records:
        (s.createDataFrame(obs_records, OBS_SCHEMA).dropDuplicates(["series_id", "date"])
          .createOrReplaceTempView("fred_obs_batch"))
        s.sql(f"""MERGE INTO {FRED_OBS} t USING fred_obs_batch u
                  ON t.series_id = u.series_id AND t.date = u.date
                  WHEN MATCHED AND NOT (t.value <=> u.value) THEN UPDATE SET
                    t.value = u.value, t.value_raw = u.value_raw, t.loaded_at = u.loaded_at
                  WHEN NOT MATCHED THEN INSERT *""")
        oh.set_watermark("fred", "observations", max(r["date"] for r in obs_records), len(obs_records))
    return summary


def run_oecd(mode="recent"):
    ensure_tables()
    s = oh.spark()
    start = "1990-01" if mode == "backfill" else f"{dt.date.today().year - 2}-01"
    dl = oh.Downloader("oecd", timeout=180)
    try:
        r = dl.get("cli", OECD_URL, params={"startPeriod": start, "format": "csvfilewithlabels"})
        text = r.content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        loaded_at = dt.datetime.utcnow()
        records = [{"ref_area": row.get("REF_AREA"), "area_name": row.get("Reference area"),
                    "time_period": row.get("TIME_PERIOD"), "obs_value": _float(row.get("OBS_VALUE")),
                    "row_json": json.dumps(row), "loaded_at": loaded_at} for row in reader]
        dl.note_rows(len(records), outcome="ok" if records else "no_data")
    finally:
        dl.flush()
    if not records:
        return {"rows": 0}
    (s.createDataFrame(records, OECD_SCHEMA).dropDuplicates(["ref_area", "time_period"])
      .createOrReplaceTempView("oecd_batch"))
    s.sql(f"""MERGE INTO {OECD_TABLE} t USING oecd_batch u
              ON t.ref_area = u.ref_area AND t.time_period = u.time_period
              WHEN MATCHED AND NOT (t.obs_value <=> u.obs_value) THEN UPDATE SET
                t.obs_value = u.obs_value, t.row_json = u.row_json, t.loaded_at = u.loaded_at
              WHEN NOT MATCHED THEN INSERT *""")
    latest = max(r["time_period"] for r in records)
    oh.set_watermark("oecd", "cli", latest, len(records))
    return {"rows": len(records), "areas": len({r["ref_area"] for r in records}),
            "first": min(r["time_period"] for r in records), "last": latest}
