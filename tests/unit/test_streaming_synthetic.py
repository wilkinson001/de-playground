import random
from datetime import datetime, timedelta, timezone
from itertools import islice

from de_playground.streaming import synthetic
from de_playground.streaming.events import to_record

START = datetime(2026, 11, 11, 10, 0, tzinfo=timezone.utc)


def test_synthetic_deterministic_and_valid():
    a = list(synthetic.raw_events(random.Random(7), START, count=50))
    b = list(synthetic.raw_events(random.Random(7), START, count=50))
    assert a == b and len(a) == 50

    records = [to_record(e) for e in a]
    assert all(r is not None for r in records)
    assert {r["wiki"] for r in records} <= set(synthetic.WIKIS)
    times = [datetime.fromisoformat(r["event_time"].replace("Z", "+00:00")) for r in records]
    assert all(t2 - t1 == timedelta(seconds=0.2) for t1, t2 in zip(times, times[1:]))
    assert times[0] == START


def test_synthetic_mix_of_bots_and_types():
    records = [to_record(e) for e in synthetic.raw_events(random.Random(1), START, count=2000)]
    bot_share = sum(r["bot"] for r in records) / len(records)
    new_share = sum(r["type"] == "new" for r in records) / len(records)
    assert 0.25 < bot_share < 0.35
    assert 0.07 < new_share < 0.13
    assert {r["type"] for r in records} == {"edit", "new"}


def test_synthetic_unbounded_without_count():
    assert len(list(islice(synthetic.raw_events(random.Random(2), START), 1000))) == 1000
