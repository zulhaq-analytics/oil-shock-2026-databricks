"""PortWatch ports loader: daily port calls and trade estimates for Gulf ports into oil_shock.bronze.portwatch_ports."""
import json
import datetime as dt

from pyspark.sql import types as T
from src import oil_http as oh

URL = ("https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/"
       "Daily_Ports_Data/FeatureServer/0/query")
TABLE = f"{oh.CATALOG}.bronze.portwatch_ports"
COUNTRIES = ["SAU", "IRQ", "KWT", "ARE", "IRN", "QAT", "OMN", "BHR"]   # Gulf producers (+ Oman, Bahrain)
BACKFILL_START = "2025-01-01"
PAGE = 1000            # PortWatch returns at most 1,000 rows per request
LOOKBACK_DAYS = 45

SCHEMA = T.StructType([
    T.StructField("date", T.StringType()),
    T.StructField("portid", T.StringType()),
    T.StructField("portname", T.StringType()),
    T.StructField("iso3", T.StringType()),
    T.StructField("attributes_json", T.StringType()),
    T.StructField("loaded_at", T.TimestampType()),
])


def ensure_table():
    oh.spark().sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
          date STRING, portid STRING, portname STRING, iso3 STRING,
          attributes_json STRING COMMENT 'All fields for the day, as returned by PortWatch (trade in metric tons)',
          loaded_at TIMESTAMP)
        COMMENT 'Raw IMF PortWatch daily port calls and trade estimates, Gulf countries. Sources: UN Global Platform; IMF PortWatch'""")


def fetch(dl, where):
    rows, offset = [], 0
    while True:
        # Sort by the server's row ID: fast and stable for paging (sorting by date + port is ~30x slower)
        params = {"where": where, "outFields": "*", "orderByFields": "ObjectId",
                  "resultOffset": offset, "resultRecordCount": PAGE, "f": "json"}
        r = dl.get("ports_daily", URL, params=params)
        body = r.json()
        if "error" in body:
            raise RuntimeError(f"PortWatch error: {body['error']}")
        page = [f["attributes"] for f in body.get("features", [])]
        dl.note_rows(len(page), outcome="ok" if page else "no_data")
        rows.extend(page)
        offset += len(page)
        if page and offset % 5000 < PAGE:
            print(f"  ...{offset:,} rows so far")
        if not page or not body.get("exceededTransferLimit"):
            return rows


def run(mode="recent"):
    """mode: 'backfill' (from BACKFILL_START) or 'recent' (last 45 days)."""
    ensure_table()
    dl = oh.Downloader("portwatch", timeout=120)
    countries = ",".join(f"'{c}'" for c in COUNTRIES)
    start = BACKFILL_START if mode == "backfill" else (dt.date.today() - dt.timedelta(days=LOOKBACK_DAYS)).isoformat()
    where = f"ISO3 IN ({countries}) AND date >= DATE '{start}'"
    try:
        rows = fetch(dl, where)
    finally:
        dl.flush()

    if not rows:
        return {"rows": 0}

    loaded_at = dt.datetime.utcnow()
    records = [{"date": str(a.get("date")), "portid": str(a.get("portid")), "portname": a.get("portname"),
                "iso3": a.get("ISO3"), "attributes_json": json.dumps(a), "loaded_at": loaded_at} for a in rows]
    df = oh.spark().createDataFrame(records, SCHEMA).dropDuplicates(["portid", "date"])
    df.createOrReplaceTempView("pwp_batch")
    oh.spark().sql(f"""
        MERGE INTO {TABLE} t USING pwp_batch s
        ON t.portid = s.portid AND t.date = s.date
        WHEN MATCHED AND t.attributes_json <> s.attributes_json THEN UPDATE SET
          t.attributes_json = s.attributes_json, t.portname = s.portname, t.iso3 = s.iso3, t.loaded_at = s.loaded_at
        WHEN NOT MATCHED THEN INSERT *""")

    latest = max(r["date"] for r in records)
    oh.set_watermark("portwatch", "ports_daily", latest, len(records))
    return {"rows": len(records), "first": min(r["date"] for r in records), "last": latest,
            "ports": len({r["portid"] for r in records})}
