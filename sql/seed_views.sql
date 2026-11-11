-- Seed-only views for the NOAA CO-OPS Sitka water-level seed.
-- BigQuery Sandbox does not support DML, so duplicate timestamps are removed
-- in a view instead of being updated in place.

CREATE OR REPLACE VIEW `tides-pipeline.noaa_coops.seed_water_levels_dedup` AS
SELECT
  station_id,
  station_name,
  product,
  timestamp,
  water_level_m,
  datum,
  units,
  sigma_m,
  flags,
  quality
FROM `tides-pipeline.noaa_coops.water_level_seed`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY station_id, product, timestamp
  ORDER BY water_level_m DESC
) = 1;

CREATE OR REPLACE VIEW `tides-pipeline.noaa_coops.seed_tide_residuals` AS
WITH paired_levels AS (
  SELECT
    station_id,
    timestamp,
    MAX(IF(product = 'observations', water_level_m, NULL)) AS observed_m,
    MAX(IF(product = 'predictions', water_level_m, NULL)) AS predicted_m
  FROM `tides-pipeline.noaa_coops.seed_water_levels_dedup`
  GROUP BY station_id, timestamp
)
SELECT
  station_id,
  timestamp,
  observed_m,
  predicted_m,
  observed_m - predicted_m AS residual_m
FROM paired_levels
WHERE observed_m IS NOT NULL
  AND predicted_m IS NOT NULL;
