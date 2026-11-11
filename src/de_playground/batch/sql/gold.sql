-- Gold: analytics aggregates for the month, recomputed from silver.
-- The pipeline MERGEs each view into its gold table on the table's key.
-- Tip % only counts card trips (payment_type 1): TLC records no tips for cash.

CREATE OR REPLACE TEMPORARY VIEW gold_daily_zone_batch AS
SELECT
    pickup_date,
    pu_location_id,
    pu_borough,
    pu_zone,
    count(*) AS trips,
    CAST(sum(total_amount) AS DECIMAL(14,2)) AS total_revenue,
    avg(CAST(trip_distance AS DOUBLE)) AS avg_trip_distance,
    avg(CASE WHEN payment_type = 1 AND fare_amount > 0
             THEN CAST(tip_amount AS DOUBLE) / CAST(fare_amount AS DOUBLE) END) AS avg_tip_pct
FROM {silver_trips}
WHERE pickup_month = '{month}'
GROUP BY pickup_date, pu_location_id, pu_borough, pu_zone;

CREATE OR REPLACE TEMPORARY VIEW gold_hourly_borough_batch AS
SELECT pickup_date, pickup_hour, pu_borough, count(*) AS trips
FROM {silver_trips}
WHERE pickup_month = '{month}'
GROUP BY pickup_date, pickup_hour, pu_borough;

CREATE OR REPLACE TEMPORARY VIEW gold_payment_tip_batch AS
SELECT
    DATE '{month_start}' AS pickup_month,
    payment_type,
    CASE payment_type
        WHEN 0 THEN 'Flex Fare' WHEN 1 THEN 'Credit card' WHEN 2 THEN 'Cash' WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute' WHEN 5 THEN 'Unknown' WHEN 6 THEN 'Voided trip' ELSE 'Other'
    END AS payment_type_name,
    count(*) AS trips,
    avg(CAST(tip_amount AS DOUBLE)) AS avg_tip_amount,
    avg(CASE WHEN payment_type = 1 AND fare_amount > 0
             THEN CAST(tip_amount AS DOUBLE) / CAST(fare_amount AS DOUBLE) END) AS avg_tip_pct
FROM {silver_trips}
WHERE pickup_month = '{month}'
GROUP BY payment_type;
