-- Silver: the month's bronze rows, cleaned, keyed, de-duplicated and enriched with zone names.
-- The pipeline then MERGEs `silver_batch` into the silver table on trip_id.
-- Dialect tokens: {epoch_us} is epoch_us (DuckDB) / unix_micros (Spark).

CREATE OR REPLACE TEMPORARY VIEW silver_batch AS
WITH clean AS (
    SELECT *
    FROM {bronze_trips}
    WHERE source_month = '{month}'
      -- TLC files contain stray trips from other months/years: keep only this month's pickups
      AND pickup_at >= TIMESTAMP '{month_start} 00:00:00'
      AND pickup_at < TIMESTAMP '{next_month_start} 00:00:00'
      AND dropoff_at > pickup_at
      AND {epoch_us}(dropoff_at) - {epoch_us}(pickup_at) <= 21600000000  -- 6 hours in microseconds
      AND trip_distance > 0 AND trip_distance <= 200
      AND fare_amount >= 0 AND total_amount >= 0
      AND (passenger_count IS NULL OR passenger_count <= 8)
),
keyed AS (
    -- TLC has no trip id, so derive one from the columns that identify a trip.
    -- Decimal casts make the string form identical in every engine.
    SELECT
        md5(concat_ws('|',
            CAST(vendor_id AS {string_type}),
            CAST({epoch_us}(pickup_at) AS {string_type}),
            CAST({epoch_us}(dropoff_at) AS {string_type}),
            CAST(pu_location_id AS {string_type}),
            CAST(do_location_id AS {string_type}),
            CAST(CAST(trip_distance AS DECIMAL(8,2)) AS {string_type}),
            CAST(CAST(total_amount AS DECIMAL(10,2)) AS {string_type})
        )) AS trip_id,
        *
    FROM clean
),
ranked AS (
    SELECT *, row_number() OVER (PARTITION BY trip_id ORDER BY ingested_at) AS rn
    FROM keyed
)
SELECT
    d.trip_id,
    '{month}' AS pickup_month,
    d.pickup_at,
    d.dropoff_at,
    CAST(d.pickup_at AS DATE) AS pickup_date,
    CAST(hour(d.pickup_at) AS {int_type}) AS pickup_hour,
    ({epoch_us}(d.dropoff_at) - {epoch_us}(d.pickup_at)) / 60000000.0 AS duration_minutes,
    d.vendor_id,
    d.passenger_count,
    CAST(d.trip_distance AS DECIMAL(8,2)) AS trip_distance,
    d.pu_location_id,
    d.do_location_id,
    pz.borough AS pu_borough,
    pz.zone AS pu_zone,
    dz.borough AS do_borough,
    dz.zone AS do_zone,
    d.payment_type,
    CAST(d.fare_amount AS DECIMAL(10,2)) AS fare_amount,
    CAST(d.tip_amount AS DECIMAL(10,2)) AS tip_amount,
    CAST(d.total_amount AS DECIMAL(10,2)) AS total_amount,
    CAST(d.airport_fee AS DECIMAL(10,2)) AS airport_fee,
    CAST(d.cbd_congestion_fee AS DECIMAL(10,2)) AS cbd_congestion_fee
FROM ranked d
LEFT JOIN {bronze_zones} pz ON pz.location_id = d.pu_location_id
LEFT JOIN {bronze_zones} dz ON dz.location_id = d.do_location_id
WHERE d.rn = 1;
