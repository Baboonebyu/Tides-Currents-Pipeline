-- Combined seed and live views. The live append table may contain overlapping
-- rows; the newest ingestion for each station/product/timestamp wins.

CREATE OR REPLACE VIEW `tides-pipeline.noaa_coops.water_levels_dedup` AS
WITH all_rows AS (
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
    quality,
    CAST(NULL AS TIMESTAMP) AS ingested_at
  FROM `tides-pipeline.noaa_coops.water_level_seed`
  UNION ALL
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
    quality,
    ingested_at
  FROM `tides-pipeline.noaa_coops.water_level_live`
)
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
  quality,
  ingested_at
FROM all_rows
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY station_id, product, timestamp
  ORDER BY ingested_at DESC NULLS LAST, water_level_m DESC
) = 1;

CREATE OR REPLACE VIEW `tides-pipeline.noaa_coops.tide_residuals` AS
WITH paired_levels AS (
  SELECT
    station_id,
    timestamp,
    MAX(IF(product = 'observations', water_level_m, NULL)) AS observed_m,
    MAX(IF(product = 'predictions', water_level_m, NULL)) AS predicted_m
  FROM `tides-pipeline.noaa_coops.water_levels_dedup`
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
