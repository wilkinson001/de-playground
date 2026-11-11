from de_playground.batch.columns import BRONZE_COLUMNS, bronze_select_list

TLC_2024_STYLE = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
    "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount",
    "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "airport_fee",
]


def test_bronze_columns_order():
    names = [c for c, _ in BRONZE_COLUMNS]
    assert names[:3] == ["vendor_id", "pickup_at", "dropoff_at"]
    assert names[-2:] == ["airport_fee", "cbd_congestion_fee"]
    assert len(names) == 20


def test_bronze_select_list_handles_missing_and_case_variants():
    duck = bronze_select_list(TLC_2024_STYLE, "duckdb")
    assert 'CAST(src."VendorID" AS INTEGER) AS vendor_id' in duck
    assert 'CAST(src."tpep_pickup_datetime" AS TIMESTAMP) AS pickup_at' in duck
    assert 'CAST(src."airport_fee" AS DOUBLE) AS airport_fee' in duck
    assert "CAST(NULL AS DOUBLE) AS cbd_congestion_fee" in duck
    assert 'CAST(src."store_and_fwd_flag" AS VARCHAR) AS store_and_fwd_flag' in duck

    spark = bronze_select_list(TLC_2024_STYLE, "spark")
    assert "CAST(src.`store_and_fwd_flag` AS STRING) AS store_and_fwd_flag" in spark
    assert "CAST(src.`VendorID` AS INT) AS vendor_id" in spark
    assert spark.count(" AS ") == 2 * 20


def test_bronze_select_list_matches_upper_case_airport_fee():
    cols = [c if c != "airport_fee" else "Airport_fee" for c in TLC_2024_STYLE]
    assert 'CAST(src."Airport_fee" AS DOUBLE) AS airport_fee' in bronze_select_list(cols, "duckdb")
