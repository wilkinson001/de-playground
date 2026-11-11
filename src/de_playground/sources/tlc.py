"""NYC TLC yellow-taxi trip records: download a month into the `raw` bucket.

Files are fetched once and never re-downloaded: the raw bucket is the landing zone that every
engine × format variant of the pipeline reads from.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass

import httpx
from botocore.client import BaseClient

from de_playground import storage
from de_playground.config import Settings
from de_playground.months import Month

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net"
ZONES_URL = f"{TLC_BASE}/misc/taxi_zone_lookup.csv"
RAW_BUCKET = "raw"
RAW_ZONES_KEY = "tlc/taxi_zone_lookup.csv"


class SourceUnavailable(Exception):
    def __init__(self, url: str, status: int):
        super().__init__(f"{url} returned HTTP {status} (month not published yet?)")
        self.url = url
        self.status = status


@dataclass(frozen=True)
class DownloadResult:
    key: str
    skipped: bool
    bytes: int


def tlc_trip_url(month: Month) -> str:
    return f"{TLC_BASE}/trip-data/yellow_tripdata_{month}.parquet"


def raw_trips_key(month: Month) -> str:
    return f"tlc/yellow/yellow_tripdata_{month}.parquet"


def download_month(
    month: Month, settings: Settings, *, client: BaseClient | None = None, http: httpx.Client | None = None
) -> DownloadResult:
    return _download(tlc_trip_url(month), raw_trips_key(month), settings, client, http)


def download_zones(
    settings: Settings, *, client: BaseClient | None = None, http: httpx.Client | None = None
) -> DownloadResult:
    return _download(ZONES_URL, RAW_ZONES_KEY, settings, client, http)


def _download(
    url: str, key: str, settings: Settings, client: BaseClient | None, http: httpx.Client | None
) -> DownloadResult:
    client = client or storage.s3_client(settings)
    if storage.object_exists(client, RAW_BUCKET, key):
        head = client.head_object(Bucket=RAW_BUCKET, Key=key)
        return DownloadResult(key=key, skipped=True, bytes=head["ContentLength"])

    http = http or httpx.Client(timeout=httpx.Timeout(30, read=300), follow_redirects=True)
    # Spool to a temp file so multi-hundred-MB months never sit in memory; upload_file
    # then does a multipart upload, and nothing lands in S3 unless the download completed.
    with tempfile.NamedTemporaryFile(suffix=".download") as tmp:
        with http.stream("GET", url) as response:
            if response.status_code != 200:
                raise SourceUnavailable(url, response.status_code)
            for chunk in response.iter_bytes(chunk_size=1 << 20):
                tmp.write(chunk)
        tmp.flush()
        size = tmp.tell()
        client.upload_file(tmp.name, RAW_BUCKET, key)
    return DownloadResult(key=key, skipped=False, bytes=size)
