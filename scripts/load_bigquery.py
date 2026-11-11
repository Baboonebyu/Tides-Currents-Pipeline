"""Batch-load normalized NOAA seed JSONL files into BigQuery Sandbox."""

from __future__ import annotations

import argparse
import hashlib
import os
import tempfile
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = REPOSITORY_ROOT / "data" / "processed" / "noaa" / "9451600"
DEFAULT_DATASET_ID = "noaa_coops"
DEFAULT_TABLE_ID = "water_level_seed"
SCHEMA = [
    bigquery.SchemaField("station_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("station_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("product", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("water_level_m", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("datum", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("units", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("sigma_m", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("flags", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("quality", "STRING", mode="NULLABLE"),
]


def combine_files(paths: list[Path], output_file) -> str:
    digest = hashlib.sha256()
    last_byte = b""
    for path in paths:
        with path.open("rb") as source_file:
            while block := source_file.read(1024 * 1024):
                output_file.write(block)
                digest.update(block)
                last_byte = block[-1:]
        if last_byte not in (b"\n", b"\r"):
            output_file.write(b"\n")
            digest.update(b"\n")
            last_byte = b"\n"
    output_file.flush()
    output_file.seek(0)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load cleaned NOAA JSONL seed files into a BigQuery table."
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("BIGQUERY_PROJECT_ID"),
        help="Google Cloud project ID (or set BIGQUERY_PROJECT_ID).",
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET_ID,
        help=f"Existing BigQuery dataset (default: {DEFAULT_DATASET_ID}).",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE_ID,
        help=f"Seed table to replace (default: {DEFAULT_TABLE_ID}).",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Folder containing cleaned JSONL files.",
    )
    parser.add_argument(
        "--location",
        default="US",
        help="BigQuery dataset location (default: US).",
    )
    args = parser.parse_args()

    if not args.project_id:
        parser.error("Provide --project-id or set BIGQUERY_PROJECT_ID")

    input_dir = args.input_dir
    if not input_dir.is_absolute():
        input_dir = REPOSITORY_ROOT / input_dir
    files = sorted(input_dir.glob("*.jsonl"))
    if not files:
        parser.error(f"No cleaned JSONL files found in {input_dir}")

    client = bigquery.Client(project=args.project_id)
    dataset_ref = bigquery.DatasetReference(args.project_id, args.dataset)
    try:
        dataset = client.get_dataset(dataset_ref)
    except NotFound as exc:
        parser.error(
            f"Dataset {args.project_id}.{args.dataset} was not found. "
            "Create it in BigQuery Studio first."
        )
        raise exc

    if dataset.location.lower() != args.location.lower():
        parser.error(
            f"Dataset location is {dataset.location}; pass "
            f"--location {dataset.location} to match it."
        )

    table_id = f"{args.project_id}.{args.dataset}.{args.table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        max_bad_records=0,
    )

    with tempfile.TemporaryFile(mode="w+b") as combined_file:
        seed_hash = combine_files(files, combined_file)
        job_id = f"sitka_seed_{seed_hash[:24]}"
        try:
            load_job = client.get_job(job_id, location=dataset.location)
            print(f"Found completed seed load job {job_id}; checking its result.")
        except NotFound:
            load_job = client.load_table_from_file(
                combined_file,
                table_id,
                job_config=job_config,
                location=dataset.location,
                job_id=job_id,
            )
        load_job.result()
        loaded_table = client.get_table(table_id)

    print(
        f"Loaded {loaded_table.num_rows} rows from {len(files)} cleaned files "
        f"into {table_id} in {dataset.location}."
    )
    print(f"Seed content SHA-256: {seed_hash}")
    print("This seed table is replaced on each successful seed load.")


if __name__ == "__main__":
    main()
