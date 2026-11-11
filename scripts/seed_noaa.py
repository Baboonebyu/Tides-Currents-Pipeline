"""Download the historical Sitka NOAA CO-OPS seed as unmodified JSON files."""

from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timedelta, timezone
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
DEFAULT_DELAY_SECONDS = 0.5


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as config_file:
        return json.load(config_file)


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Date must use YYYY-MM-DD format") from exc


def month_end(value: date) -> date:
    if value.month == 12:
        next_month = date(value.year + 1, 1, 1)
    else:
        next_month = date(value.year, value.month + 1, 1)
    return next_month - timedelta(days=1)


def monthly_chunks(start: date, end: date):
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(month_end(chunk_start), end)
        yield chunk_start, chunk_end
        chunk_start = chunk_end + timedelta(days=1)


def request_params(
    config: dict[str, Any], product: str, start: date, end: date
) -> dict[str, str]:
    params = {
        "begin_date": start.strftime("%Y%m%d"),
        "end_date": end.strftime("%Y%m%d"),
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


def fetch_json(params: dict[str, str]) -> tuple[str, dict[str, Any]]:
    url = f"{API_URL}?{urlencode(params)}"
    request = Request(
        url,
        headers={"User-Agent": f"{APPLICATION_NAME}/1.0"},
        method="GET",
    )

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                raw_text = response.read().decode("utf-8")
            payload = json.loads(raw_text)
            if isinstance(payload, dict) and payload.get("error"):
                raise RuntimeError(f"NOAA API returned an error: {payload['error']}")
            if not isinstance(payload, dict):
                raise RuntimeError("NOAA API response was not a JSON object")
            return raw_text, payload
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


def is_valid_json_response(path: Path) -> tuple[bool, int]:
    try:
        with path.open("r", encoding="utf-8") as response_file:
            payload = json.load(response_file)
    except (OSError, json.JSONDecodeError):
        return False, 0

    if not isinstance(payload, dict) or payload.get("error"):
        return False, 0
    data = payload.get("data", payload.get("predictions", []))
    return True, len(data) if isinstance(data, list) else 0


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    temporary_path = path.with_suffix(".tmp")
    with temporary_path.open("w", encoding="utf-8", newline="\n") as manifest_file:
        json.dump(manifest, manifest_file, indent=2, ensure_ascii=False)
        manifest_file.write("\n")
    temporary_path.replace(path)


def run_seed(
    start: date,
    end: date,
    output_dir: Path,
    overwrite: bool,
    delay_seconds: float,
) -> None:
    config = load_config()
    station_id = str(config["station_id"])
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "seed_manifest.json"

    if manifest_path.exists():
        try:
            with manifest_path.open("r", encoding="utf-8") as manifest_file:
                manifest = json.load(manifest_file)
        except json.JSONDecodeError:
            manifest = {}
    else:
        manifest = {}

    manifest.update(
        {
            "source": config["source"],
            "station_id": station_id,
            "station_name": config["station_name"],
            "seed_start_date": start.isoformat(),
            "seed_end_date": end.isoformat(),
            "timezone": config["timezone"],
            "units": config["units"],
            "datum": config["datum"],
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "chunks": manifest.get("chunks", {}),
        }
    )

    products = [
        ("observations", config["products"]["observations"]),
        ("predictions", config["products"]["predictions"]),
    ]
    request_count = 0

    for product_name, product in products:
        for chunk_start, chunk_end in monthly_chunks(start, end):
            start_text = chunk_start.isoformat()
            end_text = chunk_end.isoformat()
            chunk_key = f"{product_name}:{start_text}:{end_text}"
            output_path = output_dir / (
                f"{product_name}_{chunk_start:%Y-%m-%d}_{chunk_end:%Y-%m-%d}.json"
            )

            if output_path.exists() and not overwrite:
                valid, row_count = is_valid_json_response(output_path)
                if valid:
                    manifest["chunks"][chunk_key] = {
                        "file": output_path.name,
                        "rows": row_count,
                        "status": "reused",
                    }
                    write_manifest(manifest_path, manifest)
                    print(f"Reusing {output_path.name} ({row_count} rows)")
                    continue

            params = request_params(config, product, chunk_start, chunk_end)
            raw_text, payload = fetch_json(params)
            temporary_path = output_path.with_suffix(".tmp")
            with temporary_path.open("w", encoding="utf-8", newline="\n") as output_file:
                output_file.write(raw_text)
                output_file.write("\n")
            temporary_path.replace(output_path)

            data = payload.get("data", payload.get("predictions", []))
            row_count = len(data) if isinstance(data, list) else 0
            manifest["chunks"][chunk_key] = {
                "file": output_path.name,
                "rows": row_count,
                "status": "downloaded",
                "request_params": params,
            }
            manifest["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
            write_manifest(manifest_path, manifest)
            request_count += 1
            print(f"Saved {output_path.name} ({row_count} rows)")
            if delay_seconds > 0:
                time.sleep(delay_seconds)

    print(
        f"Seed download complete for station {station_id}: "
        f"{request_count} new requests; files in {output_dir}"
    )


def main() -> None:
    config = load_config()
    parser = argparse.ArgumentParser(
        description="Download NOAA CO-OPS seed data as raw monthly JSON files."
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        default=parse_date(config["seed_start_date"]),
        help="First date to download (YYYY-MM-DD; defaults to pipeline.json).",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=datetime.now(timezone.utc).date(),
        help="Last date to download (YYYY-MM-DD; defaults to today in UTC).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPOSITORY_ROOT / "data" / "raw" / "noaa" / str(config["station_id"]),
        help="Where to save raw responses (defaults under ignored data/).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Fetch and replace chunks even when valid files already exist.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help="Pause between successful requests (default: 0.5 seconds).",
    )
    args = parser.parse_args()

    if args.start_date > args.end_date:
        parser.error("--start-date must be on or before --end-date")
    if args.delay_seconds < 0:
        parser.error("--delay-seconds cannot be negative")

    output_dir = args.output_dir
    if not output_dir.is_absolute():
        output_dir = REPOSITORY_ROOT / output_dir

    run_seed(
        start=args.start_date,
        end=args.end_date,
        output_dir=output_dir,
        overwrite=args.overwrite,
        delay_seconds=args.delay_seconds,
    )


if __name__ == "__main__":
    main()
