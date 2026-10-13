"""EIA loader: pulls selected EIA API v2 series into oil_shock.bronze.eia_series."""
import time
import datetime as dt

from pyspark.sql import types as T
from src import oil_http as oh

BASE = "https://api.eia.gov/v2"
PAGE = 5000
LOOKBACK_DAYS = {"daily": 14, "weekly": 35, "monthly": 120}
TABLE = f"{oh.CATALOG}.bronze.eia_series"

def S(route, freq, code, label, facet="series"):
    return {"route": route, "freq": freq, "code": code, "label": label, "facet": facet}

SERIES = [
    S("petroleum/pri/spt", "daily", "RBRTE", "Brent physical spot"),
    S("petroleum/pri/spt", "daily", "RWTC", "WTI spot (Cushing)"),
    S("petroleum/pri/spt", "daily", "EER_EPMRU_PF4_Y35NY_DPG", "Gasoline spot, New York Harbor"),
    S("petroleum/pri/spt", "daily", "EER_EPD2DXL0_PF4_Y35NY_DPG", "Ultra-low sulfur diesel spot, New York Harbor"),
    S("petroleum/pri/spt", "daily", "EER_EPJK_PF4_RGC_DPG", "Jet fuel spot, US Gulf Coast"),
    S("petroleum/pri/spt", "daily", "EER_EPD2F_PF4_Y35NY_DPG", "Heating oil spot, New York Harbor"),
    S("petroleum/pri/fut", "daily", "RCLC1", "WTI futures, contract 1 (to Apr 2024)"),
    S("petroleum/pri/fut", "daily", "RCLC2", "WTI futures, contract 2 (to Apr 2024)"),
    S("petroleum/pri/fut", "daily", "RCLC3", "WTI futures, contract 3 (to Apr 2024)"),
    S("petroleum/pri/fut", "daily", "RCLC4", "WTI futures, contract 4 (to Apr 2024)"),
    S("natural-gas/pri/fut", "daily", "RNGWHHD", "Henry Hub natural gas spot"),
    S("petroleum/sum/sndw", "weekly", "WCESTUS1", "US commercial crude stocks"),
    S("petroleum/sum/sndw", "weekly", "WCSSTUS1", "US strategic petroleum reserve"),
    S("petroleum/sum/sndw", "weekly", "WGTSTUS1", "US gasoline stocks"),
    S("petroleum/sum/sndw", "weekly", "WDISTUS1", "US distillate stocks"),
    S("petroleum/sum/sndw", "weekly", "WCRFPUS2", "US crude production"),
    S("petroleum/sum/sndw", "weekly", "WPULEUS3", "US refinery utilization"),
    S("petroleum/sum/sndw", "weekly", "WRPUPUS2", "US total product supplied"),
    S("petroleum/sum/sndw", "weekly", "WCRIMUS2", "US crude imports"),
    S("petroleum/sum/sndw", "weekly", "WCREXUS2", "US crude exports"),
    S("petroleum/crd/drill", "monthly", "E_ERTRRO_XR0_NUS_C", "US rotary rigs in operation"),
    S("steo", "monthly", "BREPUUS", "STEO: Brent price", facet="seriesId"),
    S("steo", "monthly", "WTIPUUS", "STEO: WTI price", facet="seriesId"),
    S("steo", "monthly", "PAPR_WORLD", "STEO: world liquids production", facet="seriesId"),
    S("steo", "monthly", "PATC_WORLD", "STEO: world liquids consumption", facet="seriesId"),
    S("steo", "monthly", "PAPR_OPEC", "STEO: OPEC liquids production", facet="seriesId"),
    S("steo", "monthly", "T3_STCHANGE_WORLD", "STEO: world inventory change", facet="seriesId"),
    S("steo", "monthly", "COPR_SA", "STEO: Saudi Arabia crude production", facet="seriesId"),
    S("steo", "monthly", "COPR_IZ", "STEO: Iraq crude production", facet="seriesId"),
    S("steo", "monthly", "COPR_IR", "STEO: Iran crude production", facet="seriesId"),
    S("steo", "monthly", "COPR_KU", "STEO: Kuwait crude production", facet="seriesId"),
    S("steo", "monthly", "COPR_TC", "STEO: UAE crude production", facet="seriesId"),
    S("steo", "monthly", "COPR_AE", "STEO: UAE crude production (alt code)", facet="seriesId"),
    S("steo", "monthly", "COPR_OPEC", "STEO: OPEC crude production", facet="seriesId"),
    S("steo", "monthly", "COPS_OPEC", "STEO: OPEC surplus crude capacity", facet="seriesId"),
]

SCHEMA = T.StructType([
    T.StructField("route", T.StringType()),
    T.StructField("frequency", T.StringType()),
    T.StructField("series_id", T.StringType()),
    T.StructField("series_label", T.StringType()),
    T.StructField("series_description", T.StringType()),
    T.StructField("period", T.StringType()),
    T.StructField("value", T.DoubleType()),
    T.StructField("units", T.StringType()),
    T.StructField("vintage", T.StringType()),
    T.StructField("loaded_at", T.TimestampType()),
])


def ensure_table():
    oh.spark().sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
          route STRING, frequency STRING, series_id STRING, series_label STRING,
          series_description STRING, period STRING, value DOUBLE, units STRING,
          vintage STRING COMMENT 'STEO release month (YYYY-MM); empty for actual data',
          loaded_at TIMESTAMP)
        COMMENT 'Raw EIA API v2 series values'""")


def _start_for(freq, last_period):
    if not last_period:
        return None
    if freq == "monthly":
        d = dt.datetime.strptime(last_period, "%Y-%m").date()
    else:
        d = dt.date.fromisoformat(last_period)
    s = d - dt.timedelta(days=LOOKBACK_DAYS[freq])
    return s.strftime("%Y-%m") if freq == "monthly" else s.isoformat()


def fetch(dl, key, s, start=None):
    rows, offset = [], 0
    while True:
        params = {
            "api_key": key, "frequency": s["freq"], "data[0]": "value",
            f"facets[{s['facet']}][]": s["code"],
            "sort[0][column]": "period", "sort[0][direction]": "asc",
            "offset": offset, "length": PAGE,
        }
        if start:
            params["start"] = start
        r = dl.get(s["code"], f"{BASE}/{s['route']}/data/", params=params)
        body = r.json().get("response", {})
        page = body.get("data", [])
        total = int(body.get("total", 0) or 0)
        dl.note_rows(len(page), outcome="ok" if page else "no_data")
        rows.extend(page)
        offset += len(page)
        if not page or offset >= total:
            return rows
        time.sleep(0.3)


def _to_float(v):
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def to_records(s, rows, vintage, loaded_at):
    return [{
        "route": s["route"], "frequency": s["freq"], "series_id": s["code"],
        "series_label": s["label"],
        "series_description": x.get("series-description") or x.get("seriesDescription"),
        "period": str(x.get("period")), "value": _to_float(x.get("value")),
        "units": x.get("units") or x.get("unit"),
        "vintage": vintage, "loaded_at": loaded_at,
    } for x in rows]


def merge_bronze(records):
    df = (oh.spark().createDataFrame(records, SCHEMA)
          .dropDuplicates(["series_id", "period", "vintage"]))
    df.createOrReplaceTempView("eia_batch")
    oh.spark().sql(f"""
        MERGE INTO {TABLE} t
        USING eia_batch s
        ON t.series_id = s.series_id AND t.period = s.period AND t.vintage = s.vintage
        WHEN MATCHED AND NOT (t.value <=> s.value) THEN UPDATE SET
          t.value = s.value, t.units = s.units,
          t.series_description = s.series_description, t.loaded_at = s.loaded_at
        WHEN NOT MATCHED THEN INSERT *""")


def run(key, mode="daily", codes=None):
    """mode: 'backfill' (full history) or 'daily' (recent window). Returns a summary list."""
    oh.register_secret(key)
    ensure_table()
    today = dt.date.today()
    vintage_now = today.strftime("%Y-%m")
    dl = oh.Downloader("eia", backoff_seconds=5)
    summary, failures = [], []

    for s in SERIES:
        if codes and s["code"] not in codes:
            continue
        is_steo = s["route"] == "steo"
        try:
            if is_steo:
                start = None if mode == "backfill" else f"{today.year - 2}-01"
                vintage = vintage_now
            else:
                last = None if mode == "backfill" else oh.get_watermark("eia", s["code"])
                start = _start_for(s["freq"], last)
                vintage = ""
            loaded_at = dt.datetime.utcnow()
            rows = fetch(dl, key, s, start)
            if rows:
                merge_bronze(to_records(s, rows, vintage, loaded_at))
                last_period = vintage_now if is_steo else max(str(x["period"]) for x in rows)
                oh.set_watermark("eia", s["code"], last_period, len(rows))
            summary.append((s["code"], len(rows),
                            str(rows[0]["period"]) if rows else "",
                            str(rows[-1]["period"]) if rows else "", "ok"))
        except Exception as e:
            failures.append(s["code"])
            summary.append((s["code"], 0, "", "", f"FAILED: {oh.redact(str(e))[:120]}"))
        finally:
            dl.flush()

    if failures:
        raise RuntimeError(f"EIA load finished with failures: {failures}. Summary: {summary}")
    return summary
