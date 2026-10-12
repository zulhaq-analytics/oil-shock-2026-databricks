"""PortWatch loader: daily chokepoint transits into oil_shock.bronze.portwatch_chokepoints."""
import json
import datetime as dt

from pyspark.sql import types as T
from src import oil_http as oh

URL = ("https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/"
       "Daily_Chokepoints_Data/FeatureServer/0/query")
TABLE = f"{oh.CATALOG}.bronze.portwatch_chokepoints"
PAGE = 2000
LOOKBACK_DAYS = 45

SCHEMA = T.StructType([
    T.StructField("date", T.StringType()),
    T.StructField("portid", T.StringType()),
    T.StructField("portname", T.StringType()),
    T.StructField("attributes_json", T.StringType()),
    T.StructField("loaded_at", T.TimestampType()),
])


def ensure_table():
    oh.spark().sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
          date STRING, portid STRING, portname STRING,
          attributes_json STRING COMMENT 'All fields for the day, as returned by PortWatch',
          loaded_at TIMESTAMP)
        COMMENT 'Raw IMF PortWatch daily chokepoint transits. Sources: UN Global Platform; IMF PortWatch'""")


def fetch(dl, where):
    rows, offset = [], 0
    while True:
        params = {"where": where, "outFields": "*", "orderByFields": "date ASC, portid ASC",
                  "resultOffset": offset, "resultRecordCount": PAGE, "f": "json"}
        r = dl.get("chokepoints_daily", URL, params=params)
        body = r.json()
        if "error" in body:
            raise RuntimeError(f"PortWatch error: {body['error']}")
        page = [f["attributes"] for f in body.get("features", [])]
        dl.note_rows(len(page), outcome="ok" if page else "no_data")
        rows.extend(page)
        offset += len(page)
        if not page or not body.get("exceededTransferLimit"):
            return rows


def run(mode="recent"):
    """mode: 'backfill' (all history) or 'recent' (last 45 days)."""
    ensure_table()
    dl = oh.Downloader("portwatch", timeout=120)
    try:
        if mode == "backfill":
            where = "1=1"
        else:
            start = (dt.date.today() - dt.timedelta(days=LOOKBACK_DAYS)).isoformat()
            where = f"date >= DATE '{start}'"
        rows = fetch(dl, where)
    finally:
        dl.flush()

    if not rows:
        return {"rows": 0}

    loaded_at = dt.datetime.utcnow()
    records = [{"date": str(a.get("date")), "portid": str(a.get("portid")),
                "portname": a.get("portname"), "attributes_json": json.dumps(a),
                "loaded_at": loaded_at} for a in rows]
    df = oh.spark().createDataFrame(records, SCHEMA).dropDuplicates(["portid", "date"])
    df.createOrReplaceTempView("pw_batch")
    oh.spark().sql(f"""
        MERGE INTO {TABLE} t USING pw_batch s
        ON t.portid = s.portid AND t.date = s.date
        WHEN MATCHED AND t.attributes_json <> s.attributes_json THEN UPDATE SET
          t.attributes_json = s.attributes_json, t.portname = s.portname, t.loaded_at = s.loaded_at
        WHEN NOT MATCHED THEN INSERT *""")

    latest = max(r["date"] for r in records)
    oh.set_watermark("portwatch", "chokepoints_daily", latest, len(records))
    return {"rows": len(records), "first": min(r["date"] for r in records), "last": latest,
            "chokepoints": len({r["portid"] for r in records})}
