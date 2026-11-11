import boto3
import httpx
import pytest
from moto import mock_aws

from de_playground.config import Settings
from de_playground.months import Month
from de_playground.sources import tlc

SETTINGS = Settings.from_env(
    {"S3_ACCESS_KEY": "ak", "S3_SECRET_KEY": "sk", "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p"}
)
JAN = Month.parse("2025-01")


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="raw")
        yield client


def http_returning(status: int, body: bytes = b"PAR1data") -> tuple[httpx.Client, list]:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(status, content=body)

    return httpx.Client(transport=httpx.MockTransport(handler)), calls


def test_urls_and_keys():
    assert tlc.tlc_trip_url(JAN) == "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
    assert tlc.ZONES_URL == "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    assert tlc.raw_trips_key(JAN) == "tlc/yellow/yellow_tripdata_2025-01.parquet"
    assert tlc.RAW_ZONES_KEY == "tlc/taxi_zone_lookup.csv"


def test_download_uploads_body(s3):
    http, calls = http_returning(200, b"x" * 1000)
    result = tlc.download_month(JAN, SETTINGS, client=s3, http=http)
    assert result == tlc.DownloadResult(key=tlc.raw_trips_key(JAN), skipped=False, bytes=1000)
    assert s3.get_object(Bucket="raw", Key=result.key)["Body"].read() == b"x" * 1000
    assert len(calls) == 1


def test_download_skips_existing_object(s3):
    s3.put_object(Bucket="raw", Key=tlc.raw_trips_key(JAN), Body=b"already")
    http, calls = http_returning(200)
    result = tlc.download_month(JAN, SETTINGS, client=s3, http=http)
    assert result.skipped is True
    assert calls == []


def test_download_raises_on_missing_month(s3):
    http, _ = http_returning(403, b"<Error>AccessDenied</Error>")
    with pytest.raises(tlc.SourceUnavailable) as exc:
        tlc.download_month(JAN, SETTINGS, client=s3, http=http)
    assert exc.value.status == 403
    assert "Contents" not in s3.list_objects_v2(Bucket="raw")


def test_download_zones(s3):
    http, _ = http_returning(200, b"LocationID,Borough\n1,EWR\n")
    result = tlc.download_zones(SETTINGS, client=s3, http=http)
    assert result.key == tlc.RAW_ZONES_KEY and not result.skipped
