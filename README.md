# Sitka Water Levels Pipeline

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

The source, station, scheduled compute, data store, and historical start date are selected. Seed observations and tide predictions from January 1, 2020, then continue hourly ingestion. The repository is intended to be public so standard GitHub-hosted Actions runs are free. BigQuery Sandbox is the preferred data store, pending setup.

## Ingestion plan

GitHub Actions will run the recurring NOAA collector once per hour. Runs will request an overlapping recent window and deduplicate observations and predictions by station and timestamp. The workflow should retain run logs and report failures so a missed run can be diagnosed. Actions runners are temporary, so each run must write results to the durable data store selected for the project.

## Preferred analytical store: BigQuery Sandbox

BigQuery Sandbox is the current storage choice to evaluate. It is no-cost and requires no credit card, with a 10 GiB lifetime storage limit and 1 TiB of query processing per month. Sandbox tables expire after 60 days by default. Sandbox does not support streaming inserts or DML, so ingestion should use batch loads and analytics should deduplicate overlapping rows in a view (for example, using `ROW_NUMBER()` by station and observation timestamp). These limits are suitable for the assignment's one-week follow-up, but the table lifetime needs attention if the project runs longer.

For GitHub Actions authentication, prefer Google Cloud Workload Identity Federation (GitHub OIDC) over a long-lived service-account key. This avoids storing a Google credential as a GitHub secret.

## Historical seed

Seed both observed water levels and tide predictions beginning **2020-01-01**. Save each downloaded raw response locally under `data/raw/` and bulk-load from those saved files. The NOAA API limits 6-minute water-level requests to one month, so the observation seed must be fetched in monthly chunks; align and deduplicate the two products by station and timestamp for residual analysis.

## Configuration

The initial station and API settings are in [`config/pipeline.json`](config/pipeline.json). Downloaded data belongs under `data/`, which is excluded from Git.

## NOAA references

- [CO-OPS Data Retrieval API](https://api.tidesandcurrents.noaa.gov/api/prod/)
- [Sitka station page](https://tidesandcurrents.noaa.gov/met.html?id=9451600)
- [Sitka datums](https://tidesandcurrents.noaa.gov/datums.html?id=9451600)
