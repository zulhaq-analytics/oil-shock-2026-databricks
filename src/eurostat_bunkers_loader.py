"""Eurostat nrg_cb_oilm: oil products delivered to ships on international voyages
(international maritime bunkers), thousand tonnes, monthly, by country."""
import datetime as dt

from pyspark.sql import types as T
from src import oil_http as oh

URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_cb_oilm"
PARAMS = {"nrg_bal": "INTMARB", "siec": "O4600", "unit": "THS_T", "freq": "M"}
TABLE = f"{oh.CATALOG}.bronze.eurostat_marine_bunkers"
BACKFILL_START = "2015-01"
LOOKBACK_MONTHS = 24

SCHEMA = T.StructType([
    T.StructField("geo", T.StringType()),
    T.StructField("geo_name", T.StringType()),
    T.StructField("month", T.StringType()),
    T.StructField("siec", T.StringType()),
    T.StructField("value_kt", T.DoubleType()),
    T.StructField("dataset_updated", T.StringType()),
    T.StructField("loaded_at", T.TimestampType()),
])


def ensure_table():
    oh.spark().sql(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        geo STRING, geo_name STRING, month STRING, siec STRING, value_kt DOUBLE,
        dataset_updated STRING, loaded_at TIMESTAMP)
        COMMENT 'Raw Eurostat nrg_cb_oilm: international maritime bunkers (INTMARB), oil products (O4600), thousand tonnes'""")


def _months_back(n):
    d = dt.date.today().replace(day=1)
    y, m = d.year, d.month - n
    while m <= 0:
        y, m = y - 1, m + 12
    return f"{y}-{m:02d}"


def _parse(body):
    """Flatten a JSON-stat 2.0 response into one row per (geo, month) with a value."""
    ids, sizes, dims = body["id"], body["size"], body["dimension"]
    strides, acc = [], 1
    for s in reversed(sizes):
        strides.insert(0, acc)
        acc *= s
    pos = {d: {i: code for code, i in dims[d]["category"]["index"].items()} for d in ids}
    labels = {d: dims[d]["category"].get("label", {}) for d in ids}
    rows = []
    for key, value in (body.get("value") or {}).items():
        k, coords = int(key), {}
        for d, stride, size in zip(ids, strides, sizes):
            coords[d] = pos[d][(k // stride) % size]
        rows.append({"geo": coords["geo"], "geo_name": labels["geo"].get(coords["geo"]),
                     "month": coords["time"], "siec": coords["siec"],
                     "value_kt": float(value) if value is not None else None})
    return rows


def run(mode="recent"):
    ensure_table()
    s = oh.spark()
    since = BACKFILL_START if mode == "backfill" else _months_back(LOOKBACK_MONTHS)
    dl = oh.Downloader("eurostat", timeout=120)
    try:
        r = dl.get("marine_bunkers", URL, params={**PARAMS, "sinceTimePeriod": since})
        body = r.json()
        rows = _parse(body)
        dl.note_rows(len(rows), outcome="ok" if rows else "no_data")
    finally:
        dl.flush()
    if not rows:
        return {"rows": 0}
    loaded_at, updated = dt.datetime.utcnow(), body.get("updated")
    for x in rows:
        x["dataset_updated"], x["loaded_at"] = updated, loaded_at
    (s.createDataFrame(rows, SCHEMA).dropDuplicates(["geo", "month", "siec"])
      .createOrReplaceTempView("eurostat_bunkers_batch"))
    s.sql(f"""MERGE INTO {TABLE} t USING eurostat_bunkers_batch u
              ON t.geo = u.geo AND t.month = u.month AND t.siec = u.siec
              WHEN MATCHED AND NOT (t.value_kt <=> u.value_kt) THEN UPDATE SET
                t.value_kt = u.value_kt, t.geo_name = u.geo_name,
                t.dataset_updated = u.dataset_updated, t.loaded_at = u.loaded_at
              WHEN NOT MATCHED THEN INSERT *""")
    eu = [x["month"] for x in rows if x["geo"] == "EU27_2020"]
    latest = max(eu) if eu else max(x["month"] for x in rows)
    oh.set_watermark("eurostat", "marine_bunkers", latest, len(rows))
    return {"rows": len(rows), "countries": len({x["geo"] for x in rows}),
            "first": min(x["month"] for x in rows), "last_eu27": latest, "updated": updated}
