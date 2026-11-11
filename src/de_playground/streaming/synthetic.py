"""Synthetic Wikimedia-shaped events: the fallback when the live feed is unavailable, and test data.

Events have the same raw shape as EventStreams `recentchange`, so they go through the same
`to_record` normalisation as real ones.
"""

from __future__ import annotations

import random
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta

WIKIS = ["enwiki", "dewiki", "frwiki", "jawiki", "commonswiki", "wikidatawiki"]
_SERVERS = {
    "enwiki": "en.wikipedia.org",
    "dewiki": "de.wikipedia.org",
    "frwiki": "fr.wikipedia.org",
    "jawiki": "ja.wikipedia.org",
    "commonswiki": "commons.wikimedia.org",
    "wikidatawiki": "www.wikidata.org",
}


def raw_events(
    rng: random.Random, start: datetime, count: int | None = None, interval_s: float = 0.2
) -> Iterator[dict]:
    """Deterministic for a given seed: ~30% bot edits, ~10% new pages, the rest edits."""
    step = timedelta(seconds=interval_s)
    i = 0
    while count is None or i < count:
        wiki = rng.choice(WIKIS)
        is_new = rng.random() < 0.10
        old = None if is_new else rng.randint(200, 50_000)
        new = rng.randint(50, 5_000) if is_new else max(0, old + rng.randint(-800, 1_200))
        dt = start + step * i
        yield {
            "meta": {
                "id": str(uuid.UUID(int=rng.getrandbits(128))),
                "dt": dt.isoformat().replace("+00:00", "Z"),
                "domain": _SERVERS[wiki],
                "stream": "mediawiki.recentchange",
            },
            "type": "new" if is_new else "edit",
            "namespace": rng.choice([0, 0, 0, 1, 2, 4, 6, 14]),
            "title": f"Synthetic page {rng.randint(1, 5_000)}",
            "user": f"Bot{rng.randint(1, 20)}" if (bot := rng.random() < 0.30) else f"User{rng.randint(1, 500)}",
            "bot": bot,
            "minor": rng.random() < 0.2,
            "length": {"new": new} if is_new else {"old": old, "new": new},
            "server_name": _SERVERS[wiki],
            "wiki": wiki,
        }
        i += 1
