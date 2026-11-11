"""Normalize saved NOAA CO-OPS JSON responses into BigQuery-ready JSONL."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPOSITORY_ROOT / "config" / "pipeline.json"
DEFAULT_INPUT_DIR = REPOSITORY_ROOT / "data" / "raw" / "noaa" / "9451600"
DEFAULT_OUTPUT_DIR = REPOSITORY_ROOT / "data" / "processed" / "noaa" / "9451600"


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


def normalize_record(
    record: dict[str, Any], config: dict[str, Any], product_name: str
) -> dict[str, Any]:
    timestamp = to_utc_timestamp(str(record["t"]))
    value = as_optional_float(record.get("v"))
    if value is None:
        raise ValueError("NOAA record has no numeric water-level value")

    sigma_value = as_optional_float(record.get("s"))
    return {
        "station_id": str(config["station_id"]),
        "station_name": str(config["station_name"]),
        "product": product_name,
        "timestamp": timestamp,
        "water_level_m": value,
        "datum": str(config["datum"]),
        "units": str(config["units"]),
        "sigma_m": sigma_value,
        "flags": as_optional_text(record.get("f")),
        "quality": as_optional_text(record.get("q")),
    }


def clean_file(
    source_path: Path,
    destination_path: Path,
    config: dict[str, Any],
    product_name: str,
) -> tuple[int, int]:
    with source_path.open("r", encoding="utf-8") as source_file:
        payload = json.load(source_file)

    if not isinstance(payload, dict) or payload.get("error"):
        raise ValueError(f"Invalid NOAA response in {source_path.name}")
    response_key = "predictions" if product_name == "predictions" else "data"
    records = payload.get(response_key, [])
    if not isinstance(records, list):
        raise ValueError(f"NOAA response has no data list: {source_path.name}")

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = destination_path.with_suffix(".tmp")
    cleaned_count = 0
    skipped_count = 0
    with temporary_path.open("w", encoding="utf-8", newline="\n") as output_file:
        for record in records:
            if not isinstance(record, dict):
                skipped_count += 1
                continue
            try:
                normalized = normalize_record(record, config, product_name)
            except (KeyError, ValueError):
                skipped_count += 1
                continue
            output_file.write(json.dumps(normalized, separators=(",", ":")))
            output_file.write("\n")
            cleaned_count += 1
    temporary_path.replace(destination_path)
    return cleaned_count, skipped_count


def main() -> None:
    config = load_config()
    parser = argparse.ArgumentParser(
        description="Convert downloaded NOAA response files to normalized JSONL."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Folder containing raw NOAA JSON responses.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Folder for normalized JSONL files.",
    )
    args = parser.parse_args()
    input_dir = args.input_dir if args.input_dir.is_absolute() else REPOSITORY_ROOT / args.input_dir
    output_dir = args.output_dir if args.output_dir.is_absolute() else REPOSITORY_ROOT / args.output_dir

    if not input_dir.exists():
        parser.error(f"Raw data folder does not exist: {input_dir}")

    product_files = {
        "observations": sorted(input_dir.glob("observations_*.json")),
        "predictions": sorted(input_dir.glob("predictions_*.json")),
    }
    total_cleaned = 0
    total_skipped = 0
    file_count = 0

    for product_name, source_files in product_files.items():
        for source_path in source_files:
            destination_path = output_dir / source_path.with_suffix(".jsonl").name
            cleaned_count, skipped_count = clean_file(
                source_path, destination_path, config, product_name
            )
            total_cleaned += cleaned_count
            total_skipped += skipped_count
            file_count += 1
            print(
                f"{source_path.name}: {cleaned_count} rows written, "
                f"{skipped_count} rows skipped"
            )

    if file_count == 0:
        parser.error(f"No observation or prediction JSON files found in {input_dir}")

    print(
        f"Cleaning complete: {file_count} files, {total_cleaned} rows written, "
        f"{total_skipped} rows skipped to {output_dir}"
    )


if __name__ == "__main__":
    main()
