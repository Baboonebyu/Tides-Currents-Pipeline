"""Fetch NOAA data from each product's latest saved timestamp through now."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from google.api_core.exceptions import NotFound
from google.cloud import bigquery


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPOSITORY_ROOT / "config" / "pipeline.json"
API_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
APPLICATION_NAME = "SitkaWaterLevelsPipeline"
MAX_ATTEMPTS = 5
REQUEST_TIMEOUT_SECONDS = 60
MAX_REQUEST_DAYS = 30
REQUEST_PAUSE_SECONDS = 0.2


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as config_file:
        return json.load(config_file)


def as_optional_float(value: Any) -> float | None:
    if value in (None, "", "-"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_optional_text(value: Any) -> str | None:
    if value in (None, "", "-"):
        return None
    return str(value)


def to_utc_timestamp(value: str) -> str:
    for time_format in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(value, time_format).replace(tzinfo=timezone.utc)
            return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")
        except ValueError:
            continue
    raise ValueError(f"Unsupported NOAA timestamp: {value!r}")


def build_params(
    config: dict[str, Any], product: str, start: datetime, end: datetime
) -> dict[str, str]:
    params = {
        "begin_date": start.strftime("%Y%m%d %H:%M"),
        "end_date": end.strftime("%Y%m%d %H:%M"),
        "station": str(config["station_id"]),
        "product": product,
        "time_zone": str(config["timezone"]),
        "units": str(config["units"]),
        "format": "json",
        "application": APPLICATION_NAME,
    }
    if product == config["products"]["observations"]:
        params["datum"] = str(config["datum"])
    elif product == config["products"]["predictions"]:
        params["datum"] = str(config["datum"])
        params["interval"] = str(config["prediction_interval_minutes"])
    return params


def fetch_json(params: dict[str, str]) -> dict[str, Any]:
    request = Request(
        f"{API_URL}?{urlencode(params)}",
        headers={"User-Agent": f"{APPLICATION_NAME}/1.0"},
        method="GET",
    )
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, dict):
                raise RuntimeError("NOAA API response was not a JSON object")
            if payload.get("error"):
                raise RuntimeError(f"NOAA API returned an error: {payload['error']}")
            return payload
        except HTTPError as exc:
            retryable = exc.code == 429 or 500 <= exc.code < 600
            if not retryable or attempt == MAX_ATTEMPTS:
                raise RuntimeError(f"NOAA API HTTP error {exc.code}") from exc
            delay = min(2 ** (attempt - 1), 30)
            print(f"HTTP {exc.code}; retrying in {delay} seconds ({attempt}/{MAX_ATTEMPTS})")
            time.sleep(delay)
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            if attempt == MAX_ATTEMPTS:
                raise RuntimeError(f"NOAA API request failed: {exc}") from exc
            delay = min(2 ** (attempt - 1), 30)
            print(f"Request failed; retrying in {delay} seconds ({attempt}/{MAX_ATTEMPTS})")
            time.sleep(delay)
    raise RuntimeError("NOAA API request failed after all retries")


def get_product_watermarks(
    project_id: str, config: dict[str, Any], location: str
) -> dict[str, datetime]:
    """Return the latest saved event timestamp for each product, seed plus live."""
    client = bigquery.Client(project=project_id)
    dataset_id = str(config["storage"]["dataset_id"])
    dataset = client.get_dataset(bigquery.DatasetReference(project_id, dataset_id))
    tables = (
        "water_level_seed",
        str(config["storage"].get("live_table_id", "water_level_live")),
    )
    available_tables: list[str] = []
    for table_name in tables:
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        try:
            client.get_table(table_id)
        except NotFound:
            continue
        available_tables.append(table_id)
    if not available_tables:
        raise RuntimeError(
            f"No seed or live table found in {project_id}.{dataset_id}; "
            "load the historical seed before starting live ingestion."
        )

    union_sql = "\nUNION ALL\n".join(
        f"SELECT station_id, product, timestamp FROM `{table_id}`"
        for table_id in available_tables
    )
    query = f"""
      SELECT product, MAX(timestamp) AS latest_timestamp
      FROM ({union_sql})
      WHERE station_id = @station_id
      GROUP BY product
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "station_id", "STRING", str(config["station_id"])
            )
        ]
    )
    query_job = client.query(
        query,
        job_config=job_config,
        location=dataset.location or location,
    )
    watermarks: dict[str, datetime] = {}
    for row in query_job.result():
        latest = row["latest_timestamp"]
        if latest is None:
            continue
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        watermarks[str(row["product"])] = latest.astimezone(timezone.utc)
    return watermarks


def datetime_chunks(
    start: datetime, end: datetime, max_days: int = MAX_REQUEST_DAYS
):
    """Split catch-up ranges below NOAA's 31-day water-level request limit."""
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=max_days), end)
        yield cursor, chunk_end
        cursor = chunk_end


def normalize_record(
    record: dict[str, Any],
    config: dict[str, Any],
    product_name: str,
    ingested_at: str,
) -> dict[str, Any] | None:
    try:
        timestamp = to_utc_timestamp(str(record["t"]))
    except (KeyError, ValueError):
        return None
    value = as_optional_float(record.get("v"))
    if value is None:
        return None
    return {
        "station_id": str(config["station_id"]),
        "station_name": str(config["station_name"]),
        "product": product_name,
        "timestamp": timestamp,
        "water_level_m": value,
        "datum": str(config["datum"]),
        "units": str(config["units"]),
        "sigma_m": as_optional_float(record.get("s")),
        "flags": as_optional_text(record.get("f")),
        "quality": as_optional_text(record.get("q")),
        "ingested_at": ingested_at,
    }


def collect_rows(
    config: dict[str, Any],
    watermarks: dict[str, datetime],
    overlap_hours: int,
) -> tuple[list[dict[str, Any]], dict[str, datetime], datetime]:
    ingested_at_dt = datetime.now(timezone.utc).replace(microsecond=0)
    ingested_at = ingested_at_dt.isoformat().replace("+00:00", "Z")
    rows_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    product_windows: dict[str, datetime] = {}
    products = (
        ("observations", config["products"]["observations"], "data"),
        ("predictions", config["products"]["predictions"], "predictions"),
    )

    for product_name, product, response_key in products:
        watermark = watermarks.get(product_name)
        if watermark is None:
            start = ingested_at_dt - timedelta(hours=overlap_hours)
        else:
            start = watermark - timedelta(hours=overlap_hours)
        if start > ingested_at_dt:
            start = ingested_at_dt - timedelta(hours=overlap_hours)
        product_windows[product_name] = start
        print(
            f"{product_name} catch-up window: {start.isoformat()} through "
            f"{ingested_at_dt.isoformat()} UTC"
        )

        chunks = list(datetime_chunks(start, ingested_at_dt))
        for chunk_index, (chunk_start, chunk_end) in enumerate(chunks):
            payload = fetch_json(build_params(config, product, chunk_start, chunk_end))
            records = payload.get(response_key, [])
            if not isinstance(records, list):
                raise RuntimeError(
                    f"NOAA response has no {response_key} list for {product_name}"
                )
            for record in records:
                if not isinstance(record, dict):
                    continue
                normalized = normalize_record(
                    record, config, product_name, ingested_at
                )
                if normalized is None:
                    continue
                key = (product_name, normalized["timestamp"])
                # If a chunk boundary overlaps, retain the last response's row.
                rows_by_key[key] = normalized
            if chunk_index < len(chunks) - 1:
                time.sleep(REQUEST_PAUSE_SECONDS)
        product_count = sum(1 for name, _ in rows_by_key if name == product_name)
        print(f"Collected {product_count} usable {product_name} rows")

    return list(rows_by_key.values()), product_windows, ingested_at_dt


def main() -> None:
    config = load_config()
    parser = argparse.ArgumentParser(
        description="Catch up NOAA observations and predictions from saved BigQuery watermarks."
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID") or os.environ.get("BIGQUERY_PROJECT_ID"),
        help="Google Cloud project ID (or set GCP_PROJECT_ID).",
    )
    parser.add_argument(
        "--overlap-hours",
        type=int,
        default=int(config["ingestion"].get("overlap_hours", 1)),
        help="Hours to overlap the saved watermark (default: pipeline.json).",
    )
    parser.add_argument(
        "--location",
        default="US",
        help="BigQuery dataset location (default: US).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPOSITORY_ROOT / "data" / "live" / "noaa" / str(config["station_id"]) / "live.jsonl",
        help="Output JSONL path (defaults under ignored data/).",
    )
    args = parser.parse_args()
    if not args.project_id:
        parser.error("Provide --project-id or set GCP_PROJECT_ID")
    if args.overlap_hours < 1:
        parser.error("--overlap-hours must be at least 1")
    output_path = args.output if args.output.is_absolute() else REPOSITORY_ROOT / args.output

    watermarks = get_product_watermarks(args.project_id, config, args.location)
    rows, product_windows, end = collect_rows(config, watermarks, args.overlap_hours)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with temporary_path.open("w", encoding="utf-8", newline="\n") as output_file:
        for row in rows:
            output_file.write(json.dumps(row, separators=(",", ":")))
            output_file.write("\n")
    temporary_path.replace(output_path)

    earliest_start = min(product_windows.values())
    print(
        f"Collected {len(rows)} total rows from {earliest_start.isoformat()} "
        f"through {end.isoformat()} UTC into {output_path}"
    )


if __name__ == "__main__":
    main()
