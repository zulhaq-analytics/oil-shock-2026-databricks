"""GDELT loader: oil-relevant events from daily files (history) and 15-minute files (recent)."""
import io
import re
import csv
import zipfile
import datetime as dt

from pyspark.sql import types as T
from src import oil_http as oh

DAILY_INDEX = "https://data.gdeltproject.org/events/index.html"
DAILY_URL = "https://data.gdeltproject.org/events/{day}.export.CSV.zip"
V2_URL = "https://data.gdeltproject.org/gdeltv2/{stamp}.export.CSV.zip"
TABLE = f"{oh.CATALOG}.bronze.gdelt_events"
BACKFILL_START = "20250101"
RECENT_MAX_HOURS = 72
BATCH_FILES = 20

GEO = {"IR","IZ","SA","KU","AE","QA","MU","BA","YM","IS","RS","UP","LY","NI","VE",
       "EG","DJ","ER","AG","AO","KZ","AJ","TU","SU","SO"}
ACTORS = {"IRN","IRQ","SAU","KWT","ARE","QAT","OMN","BHR","YEM","ISR","RUS","UKR","LBY",
          "NGA","VEN","EGY","DJI","ERI","DZA","AGO","KAZ","AZE","TUR","SDN","SOM"}
CONFLICT = {"13","14","15","16","17","18","19","20"}
DIPLOMACY = {"03","04","05","06","08"}
OIL_WORDS = ("oil","crude","tanker","hormuz","refiner","pipeline","opec","petrol","gasoline",
             "diesel","lng","strait","shipping","red-sea","houthi","brent","barrel","sanction")

# Column positions: GDELT 1.0 daily files have 58 columns, GDELT 2.0 15-minute files have 61.
POS = {
    58: dict(id=0, date=1, a1code=5, a1name=6, a1cty=7, a2code=15, a2name=16, a2cty=17,
             isroot=25, code=26, base=27, root=28, quad=29, gold=30, ment=31, srcs=32, arts=33,
             tone=34, gtype=49, gname=50, gcty=51, glat=53, glon=54, added=56, url=57),
    61: dict(id=0, date=1, a1code=5, a1name=6, a1cty=7, a2code=15, a2name=16, a2cty=17,
             isroot=25, code=26, base=27, root=28, quad=29, gold=30, ment=31, srcs=32, arts=33,
             tone=34, gtype=51, gname=52, gcty=53, glat=56, glon=57, added=59, url=60),
}

SCHEMA = T.StructType([
    T.StructField("globaleventid", T.LongType()),
    T.StructField("event_date", T.StringType()),
    T.StructField("date_added", T.StringType()),
    T.StructField("actor1_code", T.StringType()),
    T.StructField("actor1_name", T.StringType()),
    T.StructField("actor1_country", T.StringType()),
    T.StructField("actor2_code", T.StringType()),
    T.StructField("actor2_name", T.StringType()),
    T.StructField("actor2_country", T.StringType()),
    T.StructField("is_root_event", T.IntegerType()),
    T.StructField("event_code", T.StringType()),
    T.StructField("event_base_code", T.StringType()),
    T.StructField("event_root_code", T.StringType()),
    T.StructField("quad_class", T.IntegerType()),
    T.StructField("goldstein", T.DoubleType()),
    T.StructField("num_mentions", T.IntegerType()),
    T.StructField("num_sources", T.IntegerType()),
    T.StructField("num_articles", T.IntegerType()),
    T.StructField("avg_tone", T.DoubleType()),
    T.StructField("geo_type", T.IntegerType()),
    T.StructField("geo_name", T.StringType()),
    T.StructField("geo_country", T.StringType()),
    T.StructField("geo_lat", T.DoubleType()),
    T.StructField("geo_long", T.DoubleType()),
    T.StructField("source_url", T.StringType()),
    T.StructField("matched_by", T.StringType()),
    T.StructField("source_file", T.StringType()),
    T.StructField("loaded_at", T.TimestampType()),
])


def ensure_table():
    cols = ", ".join(f"{f.name} {f.dataType.simpleString().upper()}" for f in SCHEMA.fields)
    oh.spark().sql(f"""CREATE TABLE IF NOT EXISTS {TABLE} ({cols})
        COMMENT 'Oil-relevant GDELT events (filtered at ingestion). Source: The GDELT Project, https://www.gdeltproject.org/'""")


def _i(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def keep(row, p):
    """Return the match reason, or None if the event is not oil-relevant."""
    mentions = _i(row[p["ment"]]) or 0
    if mentions < 2:
        return None
    url = row[p["url"]].lower()
    if any(w in url for w in OIL_WORDS):
        return "oil_word"
    place = row[p["gcty"]] in GEO or row[p["a1cty"]] in ACTORS or row[p["a2cty"]] in ACTORS
    if not place:
        return None
    root = row[p["root"]]
    if root in CONFLICT:
        return "conflict_place"
    if root in DIPLOMACY and mentions >= 5:
        return "diplomacy_place"
    return None


def parse_zip(content, source_file, loaded_at):
    records = []
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        with z.open(z.namelist()[0]) as f:
            reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"), delimiter="\t")
            for row in reader:
                p = POS.get(len(row))
                if p is None:
                    continue
                why = keep(row, p)
                if not why:
                    continue
                records.append({
                    "globaleventid": _i(row[p["id"]]), "event_date": row[p["date"]],
                    "date_added": row[p["added"]],
                    "actor1_code": row[p["a1code"]], "actor1_name": row[p["a1name"]],
                    "actor1_country": row[p["a1cty"]],
                    "actor2_code": row[p["a2code"]], "actor2_name": row[p["a2name"]],
                    "actor2_country": row[p["a2cty"]],
                    "is_root_event": _i(row[p["isroot"]]), "event_code": row[p["code"]],
                    "event_base_code": row[p["base"]], "event_root_code": row[p["root"]],
                    "quad_class": _i(row[p["quad"]]), "goldstein": _f(row[p["gold"]]),
                    "num_mentions": _i(row[p["ment"]]), "num_sources": _i(row[p["srcs"]]),
                    "num_articles": _i(row[p["arts"]]), "avg_tone": _f(row[p["tone"]]),
                    "geo_type": _i(row[p["gtype"]]), "geo_name": row[p["gname"]],
                    "geo_country": row[p["gcty"]], "geo_lat": _f(row[p["glat"]]),
                    "geo_long": _f(row[p["glon"]]), "source_url": row[p["url"]],
                    "matched_by": why, "source_file": source_file, "loaded_at": loaded_at,
                })
    return records


def _merge(records):
    if not records:
        return 0
    df = (oh.spark().createDataFrame(records, SCHEMA)
          .dropDuplicates(["globaleventid"]))
    df.createOrReplaceTempView("gdelt_batch")
    oh.spark().sql(f"""
        MERGE INTO {TABLE} t USING gdelt_batch s ON t.globaleventid = s.globaleventid
        WHEN MATCHED AND s.num_mentions > t.num_mentions THEN UPDATE SET
          t.num_mentions = s.num_mentions, t.num_sources = s.num_sources,
          t.num_articles = s.num_articles, t.avg_tone = s.avg_tone, t.loaded_at = s.loaded_at
        WHEN NOT MATCHED THEN INSERT *""")
    return len(records)


def _process(dl, items, label, progress_every=50):
    """items: list of (file_name, url). Downloads, filters and merges in batches; bulk file logging."""
    summary, batch, batch_files = [], [], []
    loaded_at = dt.datetime.utcnow()

    def flush_batch():
        _merge(batch)
        oh.log_files_bulk("gdelt", batch_files)
        dl.flush()

    for i, (name, url) in enumerate(items, start=1):
        r = dl.get(label, url, raise_on_error=False)
        if r is None:
            summary.append((name, 0, "missing"))
            continue
        recs = parse_zip(r.content, name, loaded_at)
        dl.note_rows(len(recs))
        batch.extend(recs)
        batch_files.append((name, url, len(r.content), len(recs), "loaded"))
        summary.append((name, len(recs), "ok"))
        if len(batch_files) >= BATCH_FILES:
            flush_batch()
            batch, batch_files = [], []
        if i % progress_every == 0:
            print(f"  {i}/{len(items)} files, {sum(x[1] for x in summary):,} events so far", flush=True)
    if batch_files:
        flush_batch()
    dl.flush()
    return summary


def run_backfill(start=BACKFILL_START, end=None):
    """Daily files from start (YYYYMMDD) to end (default: latest published). Skips loaded days."""
    ensure_table()
    dl = oh.Downloader("gdelt", timeout=180)
    index = dl.get("daily_index", DAILY_INDEX).text
    dl.flush()
    days = sorted(set(re.findall(r"(\d{8})\.export\.CSV\.zip", index)))
    days = [d for d in days if d >= start and (end is None or d <= end)]
    done = oh.loaded_files("gdelt")
    items = [(f"daily_{d}.zip", DAILY_URL.format(day=d)) for d in days if f"daily_{d}.zip" not in done]
    print(f"Daily files in range: {len(days)}, already loaded: {len(days) - len(items)}, to load: {len(items)}")
    summary = _process(dl, items, "daily_file")
    if days:
        oh.set_watermark("gdelt", "daily_files", days[-1], sum(x[1] for x in summary))
    return summary


def run_recent(max_hours=RECENT_MAX_HOURS):
    """15-minute files since the last loaded one (at most max_hours back)."""
    ensure_table()
    now = dt.datetime.utcnow()
    last = oh.get_watermark("gdelt", "v2_files")
    floor = now - dt.timedelta(hours=max_hours)
    start = dt.datetime.strptime(last, "%Y%m%d%H%M%S") + dt.timedelta(minutes=15) if last else floor
    start = max(start, floor).replace(second=0, microsecond=0)
    start = start.replace(minute=(start.minute // 15) * 15)
    stamps = []
    t = start
    while t <= now - dt.timedelta(minutes=20):
        stamps.append(t.strftime("%Y%m%d%H%M%S"))
        t += dt.timedelta(minutes=15)
    done = oh.loaded_files("gdelt")
    items = [(f"v2_{s}.zip", V2_URL.format(stamp=s)) for s in stamps if f"v2_{s}.zip" not in done]
    dl = oh.Downloader("gdelt", timeout=60, max_retries=2, backoff_seconds=2)
    summary = _process(dl, items, "v2_file")
    loaded = [name[3:17] for name, n, status in summary if status == "ok"]
    if loaded:
        oh.set_watermark("gdelt", "v2_files", max(loaded), sum(x[1] for x in summary))
    return summary
