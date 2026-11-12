"""Append one normalized NOAA live batch to BigQuery Sandbox."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_FILE = REPOSITORY_ROOT / "data" / "live" / "noaa" / "9451600" / "live.jsonl"
DEFAULT_DATASET_ID = "noaa_coops"
DEFAULT_TABLE_ID = "water_level_live"
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
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Append a NOAA live batch to BigQuery.")
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID") or os.environ.get("BIGQUERY_PROJECT_ID"),
        help="Google Cloud project ID (or set GCP_PROJECT_ID).",
    )
    parser.add_argument("--dataset", default=DEFAULT_DATASET_ID)
    parser.add_argument("--table", default=DEFAULT_TABLE_ID)
    parser.add_argument("--input-file", type=Path, default=DEFAULT_INPUT_FILE)
    parser.add_argument("--location", default="US")
    args = parser.parse_args()
    if not args.project_id:
        parser.error("Provide --project-id or set GCP_PROJECT_ID")

    input_file = args.input_file
    if not input_file.is_absolute():
        input_file = REPOSITORY_ROOT / input_file
    if not input_file.is_file():
        parser.error(f"Live JSONL file not found: {input_file}")
    content = input_file.read_bytes()
    if not content.strip():
        print("No rows in this NOAA window; no BigQuery load job was needed.")
        return

    content_hash = hashlib.sha256(content).hexdigest()
    job_id = f"sitka_live_{content_hash[:24]}"
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
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        max_bad_records=0,
    )
    try:
        load_job = client.get_job(job_id, location=dataset.location)
        print(f"Found existing load job {job_id}; checking its result.")
    except NotFound:
        with input_file.open("rb") as source_file:
            load_job = client.load_table_from_file(
                source_file,
                table_id,
                job_config=job_config,
                location=dataset.location,
                job_id=job_id,
            )
    load_job.result()
    loaded_table = client.get_table(table_id)
    print(
        f"Appended {load_job.output_rows or 0} rows from {input_file.name} "
        f"to {table_id} in {dataset.location}; table now has {loaded_table.num_rows} rows."
    )
    print(f"Live batch SHA-256: {content_hash}")


if __name__ == "__main__":
    main()
