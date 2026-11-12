# Sitka Water Levels Pipeline

Create for BYU 452 as a choose your own project assignment. I picked the pipeline assignment. Created with Codex

An automated data pipeline for NOAA CO-OPS water-level observations and tide predictions at Sitka, Alaska.

## Source

- **Station:** Sitka, AK (`9451600`)
- **Observations:** NOAA CO-OPS `water_level` product (6-minute interval)
- **Predictions:** NOAA CO-OPS `predictions` product
- **Datum:** MLLW (Mean Lower Low Water)
- **Units:** metric
- **Timestamps:** UTC (`time_zone=gmt`)
- **Authentication:** NOAA's public CO-OPS API does not require an API key.

The station offers both water-level measurements and NOAA tide predictions, so the pipeline can align them by timestamp and calculate the residual (observed minus predicted). NOAA provides preliminary 6-minute water levels as they arrive and verifies water-level data monthly. The collector should therefore allow overlap and updates to earlier observations.

## Project status

The historical seed is loaded, the Workload Identity Federation connection is configured, and a live batch has been queried from BigQuery. The repository is intended to be public so standard GitHub-hosted Actions runs are free. GitHub Actions is scheduled to continue the live load every 15 minutes.

## Ingestion plan

GitHub Actions runs the NOAA collector every 15 minutes. Before fetching, the collector reads the latest saved event timestamp for each product from the union of `water_level_seed` and `water_level_live`. It requests data from that timestamp minus a one-hour overlap through the current time. This saved watermark means a failed or delayed run does not advance the checkpoint: after NOAA recovers, the next successful run fetches the missing interval automatically. The same process closes the seed-to-live gap.

The collector retries temporary NOAA errors and splits catch-up ranges into requests of at most 30 days, within NOAA's 31-day water-level request limit. If a run still fails, Actions records a failed run and the next scheduled run retries from the last data timestamp in BigQuery. Empty API responses don't move that product's watermark, so the same range is checked again on the next run.

Each successful run batch-appends normalized rows to `water_level_live`, with an `ingested_at` timestamp. Overlapping records are expected; `sql/live_views.sql` combines this table with the historical seed and keeps the latest ingested copy for each station, product, and observation timestamp. The pipeline uses BigQuery load jobs, not streaming inserts or DML. A manual workflow run uses the same watermark logic as a scheduled run.

## Preferred analytical store: BigQuery Sandbox

BigQuery Sandbox is the current storage choice to evaluate. The planned dataset is `noaa_coops`, named for the NOAA CO-OPS source so it can contain multiple stations. It is no-cost and requires no credit card, with a 10 GiB lifetime storage limit and 1 TiB of query processing per month. Sandbox tables expire after 60 days by default. Sandbox does not support streaming inserts or DML, so ingestion should use batch loads and analytics should deduplicate overlapping rows in a view (for example, using `ROW_NUMBER()` by station and observation timestamp). These limits are suitable for the assignment's one-week follow-up, but the table lifetime needs attention if the project runs longer.

For GitHub Actions authentication, prefer Google Cloud Workload Identity Federation (GitHub OIDC) over a long-lived service-account key. This avoids storing a Google credential as a GitHub secret.

## Historical seed

Seed both observed water levels and tide predictions beginning **2020-01-01**. Save each downloaded raw response locally under `data/raw/` and bulk-load from those saved files. The NOAA API limits 6-minute water-level requests to one month, so the observation seed must be fetched in monthly chunks; align and deduplicate the two products by station and timestamp for residual analysis.

Run the downloader from the repository root with Python:

```powershell
python scripts/seed_noaa.py
```

The end date defaults to today in UTC. You can set a fixed end date with `--end-date YYYY-MM-DD`. The script stores one raw JSON response per product and month under `data/raw/noaa/9451600/`, writes a manifest with request metadata and row counts, retries temporary failures, and skips valid chunks already on disk. The data directory is ignored by Git.

After the seed finishes downloading, normalize its files for BigQuery with:

```powershell
python scripts/clean_noaa.py
```

The cleaner writes one newline-delimited JSON file per raw chunk under `data/processed/noaa/9451600/`. It converts NOAA timestamps to explicit UTC timestamps, standardizes the water level in meters, and preserves station, datum, product, uncertainty, and quality/flag fields. Rows without a usable timestamp or numeric value are skipped and counted.

Install the BigQuery client library and load the cleaned seed into the existing `noaa_coops` dataset with:

```powershell
pip install -r requirements.txt
gcloud auth application-default login
python scripts/load_bigquery.py --project-id YOUR_PROJECT_ID
```

Replace `YOUR_PROJECT_ID` with the ID of the `tides-pipeline` project. The loader uses your local Google Application Default Credentials, submits one batch load job, and writes to `water_level_seed`. A successful seed load replaces that seed table, making a rerun safe from duplicate seed rows. The later scheduled feed will go to a separate table.

Run [`sql/seed_views.sql`](sql/seed_views.sql) in BigQuery Studio for seed-only views. After the live table has been created by the first successful workflow run, run [`sql/live_views.sql`](sql/live_views.sql) to create combined seed/live deduplication and residual views.

## Live workflow setup

The `.github/workflows/noaa-live.yml` workflow uses Workload Identity Federation through the `github-actions-bq` service account. The provider maps `attribute.repository=assertion.repository` and `attribute.ref=assertion.ref`, and its condition allows only `Baboonebyu/Tides-Currents-Pipeline` on `master`. The service account grants `roles/iam.workloadIdentityUser` to the principal set for that repository attribute. It also needs `roles/bigquery.jobUser` on the project and `roles/bigquery.dataEditor` on the `noaa_coops` dataset.

Add these nonsecret identifiers under the repository's **Settings → Secrets and variables → Actions → Variables**:

| Variable | Value |
| --- | --- |
| `WIF_PROVIDER` | `projects/572805999914/locations/global/workloadIdentityPools/github-actions/providers/github` |
| `WIF_SERVICE_ACCOUNT` | `github-actions-bq@tides-pipeline.iam.gserviceaccount.com` |
| `GCP_PROJECT_ID` | `tides-pipeline` |

To run the live collector locally, authenticate with Application Default Credentials and run:

```powershell
python scripts/ingest_noaa_live.py --project-id tides-pipeline
python scripts/load_bigquery_live.py --project-id tides-pipeline
```

The collector reads the product watermarks from BigQuery, fetches from those timestamps with a one-hour overlap, and writes to ignored `data/live/`. The loader appends that JSONL batch to `tides-pipeline.noaa_coops.water_level_live` with a deterministic BigQuery load job ID. A full day of API downtime is backfilled on the next successful run, with repeats removed from the analytical view.

## Configuration

The initial station and API settings are in [`config/pipeline.json`](config/pipeline.json). Downloaded data belongs under `data/`, which is excluded from Git.

## NOAA references

- [CO-OPS Data Retrieval API](https://api.tidesandcurrents.noaa.gov/api/prod/)
- [Sitka station page](https://tidesandcurrents.noaa.gov/met.html?id=9451600)
- [Sitka datums](https://tidesandcurrents.noaa.gov/datums.html?id=9451600)
