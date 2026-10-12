"""Trader positions (Commitments of Traders): CFTC (NYMEX) and ICE Futures Europe."""
import csv
import io
import json
import datetime as dt

from pyspark.sql import types as T
from src import oil_http as oh

CFTC_URL = "https://publicreporting.cftc.gov/resource/72hh-3qpy.json"
CFTC_CODES = {
    "067651": "WTI crude oil (NYMEX)",
    "111659": "RBOB gasoline (NYMEX)",
    "022651": "NY Harbor ULSD diesel (NYMEX)",
}
CFTC_LOOKBACK_DAYS = 56
CFTC_TABLE = f"{oh.CATALOG}.bronze.cftc_cot"

ICE_URL = "https://www.ice.com/publicdocs/futures/COTHist{year}.csv"
ICE_FIRST_YEAR = 2011
ICE_KEEP = ("Brent", "Gasoil", "Gas Oil", "Dubai")
ICE_TABLE = f"{oh.CATALOG}.bronze.ice_cot"

SCHEMA = T.StructType([
    T.StructField("market_code", T.StringType()),
    T.StructField("market_name", T.StringType()),
    T.StructField("report_date", T.StringType()),
    T.StructField("row_json", T.StringType()),
    T.StructField("source_file", T.StringType()),
    T.StructField("loaded_at", T.TimestampType()),
])


def ensure_tables():
    for table, note in [(CFTC_TABLE, "Raw CFTC disaggregated futures-only COT rows (NYMEX oil)"),
                        (ICE_TABLE, "Raw ICE Futures Europe COT rows (Brent, gasoil, Dubai)")]:
        oh.spark().sql(f"""
            CREATE TABLE IF NOT EXISTS {table} (
              market_code STRING, market_name STRING, report_date STRING,
              row_json STRING COMMENT 'All fields for the week, as published',
              source_file STRING, loaded_at TIMESTAMP)
            COMMENT '{note}'""")


def _merge(table, records):
    if not records:
        return 0
    df = (oh.spark().createDataFrame(records, SCHEMA)
          .dropDuplicates(["market_name", "report_date"]))
    df.createOrReplaceTempView("cot_batch")
    oh.spark().sql(f"""
        MERGE INTO {table} t USING cot_batch s
        ON t.market_name = s.market_name AND t.report_date = s.report_date
        WHEN MATCHED AND t.row_json <> s.row_json THEN UPDATE SET
          t.row_json = s.row_json, t.source_file = s.source_file, t.loaded_at = s.loaded_at
        WHEN NOT MATCHED THEN INSERT *""")
    return df.count()


def run_cftc(mode="recent"):
    ensure_tables()
    dl = oh.Downloader("cftc", timeout=120)
    codes = ",".join(f"'{c}'" for c in CFTC_CODES)
    where = f"cftc_contract_market_code in({codes})"
    if mode != "backfill":
        start = (dt.date.today() - dt.timedelta(days=CFTC_LOOKBACK_DAYS)).isoformat()
        where += f" AND report_date_as_yyyy_mm_dd >= '{start}T00:00:00'"
    rows, offset, page = [], 0, 50000
    try:
        while True:
            r = dl.get("disaggregated_fut", CFTC_URL, params={
                "$where": where, "$order": "report_date_as_yyyy_mm_dd ASC",
                "$limit": page, "$offset": offset})
            batch = r.json()
            dl.note_rows(len(batch), outcome="ok" if batch else "no_data")
            rows.extend(batch)
            offset += len(batch)
            if len(batch) < page:
                break
    finally:
        dl.flush()

    loaded_at = dt.datetime.utcnow()
    records = [{
        "market_code": x.get("cftc_contract_market_code"),
        "market_name": x.get("market_and_exchange_names"),
        "report_date": str(x.get("report_date_as_yyyy_mm_dd", ""))[:10],
        "row_json": json.dumps(x), "source_file": "cftc_api", "loaded_at": loaded_at,
    } for x in rows]
    n = _merge(CFTC_TABLE, records)
    if records:
        oh.set_watermark("cftc", "disaggregated_fut", max(r["report_date"] for r in records), n)
    return {"rows": n,
            "first": min((r["report_date"] for r in records), default=""),
            "last": max((r["report_date"] for r in records), default=""),
            "markets": sorted({r["market_name"] for r in records})}


def _ice_date(text):
    try:
        return dt.datetime.strptime(text.strip(), "%m/%d/%Y").date().isoformat()
    except ValueError:
        return text.strip()


def run_ice(mode="recent"):
    ensure_tables()
    this_year = dt.date.today().year
    years = range(ICE_FIRST_YEAR, this_year + 1) if mode == "backfill" else [this_year - 1, this_year]
    dl = oh.Downloader("ice", timeout=120)
    summary = []
    for year in years:
        file_name = f"COTHist{year}.csv"
        if year < this_year - 1 and oh.file_already_loaded("ice", file_name):
            summary.append((file_name, 0, "skipped (already loaded)"))
            continue
        try:
            r = dl.get("cot_csv", ICE_URL.format(year=year), raise_on_error=False)
            if r is None:
                summary.append((file_name, 0, "not available"))
                continue
            text = r.content.decode("utf-8-sig", errors="replace")
            reader = csv.reader(io.StringIO(text))
            header = [h.strip() for h in next(reader)]
            loaded_at = dt.datetime.utcnow()
            records = []
            for row in reader:
                if not row or not any(k in row[0] for k in ICE_KEEP):
                    continue
                records.append({
                    "market_code": row[0].split(" - ")[0].strip(),
                    "market_name": row[0].strip(),
                    "report_date": _ice_date(row[2]) if len(row) > 2 else "",
                    "row_json": json.dumps(dict(zip(header, row))),
                    "source_file": file_name, "loaded_at": loaded_at,
                })
            dl.note_rows(len(records))
            n = _merge(ICE_TABLE, records)
            oh.log_file("ice", file_name, ICE_URL.format(year=year), len(r.content), n, "loaded")
            summary.append((file_name, n, "ok"))
        finally:
            dl.flush()
    latest = oh.spark().sql(f"SELECT MAX(report_date) AS d FROM {ICE_TABLE}").collect()[0]["d"]
    if latest:
        oh.set_watermark("ice", "cot", latest, sum(x[1] for x in summary))
    return summary
