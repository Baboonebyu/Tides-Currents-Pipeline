"""Fetch the previous hour of Sitka observations and predictions as JSONL."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPOSITORY_ROOT / "config" / "pipeline.json"
API_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
APPLICATION_NAME = "SitkaWaterLevelsPipeline"
MAX_ATTEMPTS = 5
REQUEST_TIMEOUT_SECONDS = 60


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


def collect_rows(config: dict[str, Any], lookback_hours: int) -> tuple[list[dict[str, Any]], datetime, datetime]:
    ingested_at_dt = datetime.now(timezone.utc).replace(microsecond=0)
    start = ingested_at_dt - timedelta(hours=lookback_hours)
    rows: list[dict[str, Any]] = []
    products = (
        ("observations", config["products"]["observations"], "data"),
        ("predictions", config["products"]["predictions"], "predictions"),
    )
    for product_name, product, response_key in products:
        payload = fetch_json(build_params(config, product, start, ingested_at_dt))
        records = payload.get(response_key, [])
        if not isinstance(records, list):
            raise RuntimeError(f"NOAA response has no {response_key} list for {product_name}")
        product_rows = [
            normalized
            for record in records
            if isinstance(record, dict)
            and (normalized := normalize_record(
                record, config, product_name, ingested_at_dt.isoformat().replace("+00:00", "Z")
            )) is not None
        ]
        rows.extend(product_rows)
        print(f"Fetched {len(product_rows)} usable {product_name} rows")
    return rows, start, ingested_at_dt


def main() -> None:
    config = load_config()
    parser = argparse.ArgumentParser(
        description="Fetch recent NOAA observations and predictions for BigQuery."
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=int(config["ingestion"].get("lookback_hours", 1)),
        help="Number of recent hours to fetch (default: pipeline.json).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPOSITORY_ROOT / "data" / "live" / "noaa" / str(config["station_id"]) / "live.jsonl",
        help="Output JSONL path (defaults under ignored data/).",
    )
    args = parser.parse_args()
    if args.lookback_hours < 1:
        parser.error("--lookback-hours must be at least 1")
    output_path = args.output if args.output.is_absolute() else REPOSITORY_ROOT / args.output

    rows, start, end = collect_rows(config, args.lookback_hours)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with temporary_path.open("w", encoding="utf-8", newline="\n") as output_file:
        for row in rows:
            output_file.write(json.dumps(row, separators=(",", ":")))
            output_file.write("\n")
    temporary_path.replace(output_path)
    print(
        f"Collected {len(rows)} total rows for {start.isoformat()} through "
        f"{end.isoformat()} UTC into {output_path}"
    )


if __name__ == "__main__":
    main()
