import pytest

from de_playground import storage
from de_playground.months import Month
from de_playground.sources import tlc

pytestmark = pytest.mark.integration


def test_download_real_month(settings):
    jan = Month.parse("2025-01")
    first = tlc.download_month(jan, settings)
    client = storage.s3_client(settings)
    assert storage.object_exists(client, tlc.RAW_BUCKET, tlc.raw_trips_key(jan))
    assert first.bytes > 10 * 1024 * 1024
    assert tlc.download_month(jan, settings).skipped is True
    assert tlc.download_zones(settings).key == tlc.RAW_ZONES_KEY
