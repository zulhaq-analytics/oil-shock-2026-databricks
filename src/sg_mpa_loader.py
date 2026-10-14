"""Singapore MPA monthly port data from data.gov.sg: bunker sales and vessel arrivals."""
import time
import datetime as dt

from pyspark.sql import types as T
from src import oil_http as oh

API = "https://data.gov.sg/api/action/datastore_search"
TABLE = f"{oh.CATALOG}.bronze.sg_mpa_monthly"
PAGE = 1000
PAUSE_SECONDS = 2

# name: (data.gov.sg dataset id, category field or None, {source field: measure name})
DATASETS = {
    "bunker_total": ("d_89d2874dad74a273270369334f1e7d28", None,
                     {"bunker_sales": "bunker_sales_kt"}),
    "bunker_by_type": ("d_4f5abbf4486bf8e52bbed3be56dde562", "bunker_type",
                       {"bunker_sales": "bunker_sales_kt"}),
    "arrivals_total": ("d_d48c5a038904f6da3c603cd854b6c191", None,
                       {"number_of_vessels": "vessels", "gross_tonnage": "gross_tonnage_kgt"}),
    "arrivals_by_type": ("d_8f264219109e61fffa87ac64dd5a9a65", "vessel_type",
                         {"number_of_vessels": "vessels", "gross_tonnage": "gross_tonnage_kgt"}),
}

SCHEMA = T.StructType([
    T.StructField("dataset", T.StringType()),
    T.StructField("month", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("measure", T.StringType()),
    T.StructField("value", T.DoubleType()),
    T.StructField("value_raw", T.StringType()),
    T.StructField("loaded_at", T.TimestampType()),
])


def ensure_table():
    oh.spark().sql(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        dataset STRING, month STRING, category STRING, measure STRING,
        value DOUBLE, value_raw STRING, loaded_at TIMESTAMP)
        COMMENT 'Raw Singapore MPA monthly data (data.gov.sg): bunker sales in thousand tonnes, vessel arrivals over 75 GT'""")


def _float(v):
    try:
        return float(v) if v not in (None, "", "na", "-") else None
    except (TypeError, ValueError):
        return None


def _fetch(dl, name, resource_id):
    rows, offset = [], 0
    while True:
        r = dl.get(name, API, params={"resource_id": resource_id, "limit": PAGE, "offset": offset})
        body = r.json()
        if not body.get("success", False):
            raise RuntimeError(f"[sg_mpa/{name}] data.gov.sg said not successful: {str(body)[:300]}")
        result = body.get("result", {})
        batch = result.get("records", [])
        dl.note_rows(len(batch), outcome="ok" if batch else "no_data")
        rows.extend(batch)
        offset += len(batch)
        if not batch or offset >= int(result.get("total", 0)):
            return rows
        time.sleep(PAUSE_SECONDS)


def run(mode="recent"):
    """Both modes pull the full history: each dataset is small (under 10,000 rows) and MPA revises past months."""
    ensure_table()
    s = oh.spark()
    dl = oh.Downloader("sg_mpa", timeout=90)
    loaded_at = dt.datetime.utcnow()
    records, summary = [], []
    try:
        for name, (resource_id, cat_field, measures) in DATASETS.items():
            rows = _fetch(dl, name, resource_id)
            for x in rows:
                for field, measure in measures.items():
                    records.append({
                        "dataset": name, "month": x.get("month"),
                        "category": (x.get(cat_field) or "Unknown") if cat_field else "All",
                        "measure": measure, "value": _float(x.get(field)),
                        "value_raw": x.get(field), "loaded_at": loaded_at})
            months = [x.get("month") for x in rows if x.get("month")]
            summary.append((name, len(rows), min(months) if months else "", max(months) if months else ""))
            time.sleep(PAUSE_SECONDS)
    finally:
        dl.flush()

    if not records:
        return summary
    (s.createDataFrame(records, SCHEMA)
      .dropDuplicates(["dataset", "month", "category", "measure"])
      .createOrReplaceTempView("sg_mpa_batch"))
    s.sql(f"""MERGE INTO {TABLE} t USING sg_mpa_batch u
              ON t.dataset = u.dataset AND t.month = u.month
             AND t.category = u.category AND t.measure = u.measure
              WHEN MATCHED AND NOT (t.value <=> u.value) THEN UPDATE SET
                t.value = u.value, t.value_raw = u.value_raw, t.loaded_at = u.loaded_at
              WHEN NOT MATCHED THEN INSERT *""")
    oh.set_watermark("sg_mpa", "monthly", max(r["month"] for r in records if r["month"]), len(records))
    return summary
