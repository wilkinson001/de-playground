-- Bronze: the month's raw records as-is (normalised column names/types) plus load metadata.
-- Re-running a month replaces that month's rows, so it never duplicates.
-- `raw_trips` / `raw_zones` are temp views the pipeline registers over the raw files.

DELETE FROM {bronze_trips} WHERE source_month = '{month}';

INSERT INTO {bronze_trips} ({bronze_trips_cols})
SELECT r.*, '{month}', '{source_file}', CAST(current_timestamp AS TIMESTAMP)
FROM raw_trips r;

-- The zone lookup is a small dimension: reload it whole.
DELETE FROM {bronze_zones};

INSERT INTO {bronze_zones} (location_id, borough, zone, service_zone)
SELECT CAST(LocationID AS {int_type}), CAST(Borough AS {string_type}), CAST(Zone AS {string_type}),
       CAST(service_zone AS {string_type})
FROM raw_zones;
