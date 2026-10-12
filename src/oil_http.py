"""Shared download helper for the oil-shock project: retries, call logging,
key redaction, bookmarks (watermarks) and file log."""
import time
import uuid
import datetime as dt

import requests
from pyspark.sql import SparkSession
from pyspark.sql import types as T

CATALOG = "oil_shock"
RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}
_SECRETS = []


def spark():
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def register_secret(value):
    """Remember a secret so it is never written to logs."""
    if value and value not in _SECRETS:
        _SECRETS.append(value)


def redact(text):
    text = "" if text is None else str(text)
    for s in _SECRETS:
        text = text.replace(s, "***")
    return text


LOG_SCHEMA = T.StructType([
    T.StructField("call_id", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("dataset", T.StringType()),
    T.StructField("request", T.StringType()),
    T.StructField("status_code", T.IntegerType()),
    T.StructField("outcome", T.StringType()),
    T.StructField("rows_returned", T.LongType()),
    T.StructField("bytes", T.LongType()),
    T.StructField("duration_ms", T.LongType()),
    T.StructField("error_message", T.StringType()),
    T.StructField("called_at", T.TimestampType()),
])

FILE_LOG_SCHEMA = T.StructType([
    T.StructField("source", T.StringType()),
    T.StructField("file_name", T.StringType()),
    T.StructField("file_path", T.StringType()),
    T.StructField("bytes", T.LongType()),
    T.StructField("rows_loaded", T.LongType()),
    T.StructField("status", T.StringType()),
    T.StructField("logged_at", T.TimestampType()),
])


class Downloader:
    """HTTP GET with retries and buffered call logging (call flush() at the end)."""

    def __init__(self, source, timeout=90, max_retries=4, backoff_seconds=5, user_agent=None):
        self.source = source
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.session = requests.Session()
        self.session.headers["User-Agent"] = user_agent or "oil-shock-portfolio-project (zulhaq2@gmail.com)"
        self._buffer = []

    def get(self, dataset, url, params=None, headers=None, stream=False, raise_on_error=True):
        request_text = redact(requests.Request("GET", url, params=params).prepare().url)
        last_error, response = None, None
        for attempt in range(1, self.max_retries + 1):
            start = time.time()
            try:
                response = self.session.get(url, params=params, headers=headers,
                                            timeout=self.timeout, stream=stream)
                status = response.status_code
            except requests.RequestException as e:
                status, last_error = None, f"{type(e).__name__}: {e}"
            elapsed = int((time.time() - start) * 1000)

            retryable = status is None or status in RETRY_STATUS
            if retryable and attempt < self.max_retries:
                time.sleep(self.backoff_seconds * attempt)
                continue

            if status is not None and status < 400:
                size = None if stream else len(response.content)
                self._log(dataset, request_text, status, "ok", None, size, elapsed, None)
                return response

            if status is not None:
                last_error = f"HTTP {status}: {response.text[:300]}"
            self._log(dataset, request_text, status, "error", None, None, elapsed, redact(last_error))
            break

        if raise_on_error:
            raise RuntimeError(f"[{self.source}/{dataset}] failed after {self.max_retries} tries: {redact(last_error)}")
        return None

    def note_rows(self, rows, outcome=None):
        """Record how many rows the last call produced (and optionally change its outcome)."""
        if self._buffer:
            self._buffer[-1]["rows_returned"] = int(rows)
            if outcome:
                self._buffer[-1]["outcome"] = outcome

    def _log(self, dataset, request_text, status, outcome, rows, size, ms, error):
        self._buffer.append({
            "call_id": str(uuid.uuid4()), "source": self.source, "dataset": dataset,
            "request": request_text, "status_code": status, "outcome": outcome,
            "rows_returned": rows, "bytes": size, "duration_ms": ms,
            "error_message": error, "called_at": dt.datetime.utcnow(),
        })

    def flush(self):
        """Write buffered call logs to oil_shock.ops.api_call_log."""
        if not self._buffer:
            return 0
        n = len(self._buffer)
        (spark().createDataFrame(self._buffer, LOG_SCHEMA)
            .write.mode("append").saveAsTable(f"{CATALOG}.ops.api_call_log"))
        self._buffer = []
        return n


def get_watermark(source, dataset):
    rows = spark().sql(
        f"SELECT last_period FROM {CATALOG}.ops.load_watermark WHERE source = :s AND dataset = :d",
        args={"s": source, "d": dataset}).collect()
    return rows[0]["last_period"] if rows else None


def set_watermark(source, dataset, last_period, rows_last_run):
    spark().sql(f"""
        MERGE INTO {CATALOG}.ops.load_watermark t
        USING (SELECT :s AS source, :d AS dataset, :p AS last_period,
                      current_timestamp() AS last_run_at, CAST(:r AS BIGINT) AS rows_last_run) u
        ON t.source = u.source AND t.dataset = u.dataset
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """, args={"s": source, "d": dataset, "p": str(last_period), "r": int(rows_last_run)})


def file_already_loaded(source, file_name):
    n = spark().sql(
        f"SELECT COUNT(*) AS n FROM {CATALOG}.ops.file_log "
        "WHERE source = :s AND file_name = :f AND status = 'loaded'",
        args={"s": source, "f": file_name}).collect()[0]["n"]
    return n > 0


def loaded_files(source):
    """All file names already loaded for a source, in one query."""
    rows = spark().sql(
        f"SELECT DISTINCT file_name FROM {CATALOG}.ops.file_log WHERE source = :s AND status = 'loaded'",
        args={"s": source}).collect()
    return {r["file_name"] for r in rows}


def log_file(source, file_name, file_path, size, rows_loaded, status):
    spark().sql(f"""
        INSERT INTO {CATALOG}.ops.file_log
        VALUES (:s, :f, :p, CAST(:b AS BIGINT), CAST(:r AS BIGINT), :st, current_timestamp())
    """, args={"s": source, "f": file_name, "p": file_path,
               "b": int(size or 0), "r": int(rows_loaded or 0), "st": status})


def log_files_bulk(source, entries):
    """entries: list of (file_name, file_path, size, rows_loaded, status). One insert for all."""
    if not entries:
        return 0
    now = dt.datetime.utcnow()
    rows = [{"source": source, "file_name": n, "file_path": p, "bytes": int(b or 0),
             "rows_loaded": int(r or 0), "status": st, "logged_at": now}
            for n, p, b, r, st in entries]
    (spark().createDataFrame(rows, FILE_LOG_SCHEMA)
        .write.mode("append").saveAsTable(f"{CATALOG}.ops.file_log"))
    return len(rows)
