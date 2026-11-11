import json
from itertools import islice

import httpx

from de_playground.streaming import producer
from de_playground.streaming.events import RECORD_FIELDS
from tests.unit.test_streaming_events import EDIT


class ListSink:
    def __init__(self):
        self.sent: list[tuple[str, dict]] = []

    def send(self, key, value):
        self.sent.append((key, value))

    def flush(self):
        pass


def sse(*events: tuple[str, dict]) -> bytes:
    return "".join(f"id: {i}\ndata: {json.dumps(e)}\n\n" for i, e in events).encode()


def edit(event_id: str, wiki: str = "enwiki", domain: str = "en.wikipedia.org") -> dict:
    return {**EDIT, "wiki": wiki, "meta": {**EDIT["meta"], "id": event_id, "domain": domain}}


class DropsAfter(httpx.SyncByteStream):
    """Yields the body, then fails as if the connection dropped."""

    def __init__(self, body: bytes):
        self.body = body

    def __iter__(self):
        yield self.body
        raise httpx.ReadError("connection reset")


def no_sleep(_seconds):
    pass


def test_run_wikimedia_sends_normalised_records():
    body = sse(("1", edit("a", "enwiki")), ("2", edit("b", domain="canary")), ("3", edit("c", "dewiki")))
    http = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(200, content=body)))
    sink = ListSink()
    assert producer.run(sink, "wikimedia", max_events=2, http=http, sleep=no_sleep) == 2
    assert [k for k, _ in sink.sent] == ["enwiki", "dewiki"]
    assert all(list(v) == RECORD_FIELDS for _, v in sink.sent)


def test_reconnect_resumes_from_last_event_id():
    requests: list[httpx.Request] = []

    def handler(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(200, stream=DropsAfter(sse(("e1", edit("a")))))
        return httpx.Response(200, content=sse(("e2", edit("b"))))

    http = httpx.Client(transport=httpx.MockTransport(handler))
    records = list(islice(producer.wikimedia_records(http, sleep=no_sleep), 2))
    assert [r["event_id"] for r in records] == ["a", "b"]
    assert "last-event-id" not in requests[0].headers
    assert requests[1].headers["last-event-id"] == "e1"


def test_auto_falls_back_to_synthetic():
    http = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(503)))
    sink = ListSink()
    assert producer.run(sink, "auto", max_events=5, http=http, rng_seed=1, sleep=no_sleep) == 5
    assert all(list(v) == RECORD_FIELDS for _, v in sink.sent)


def test_wikimedia_only_gives_up_after_retries():
    calls = []
    http = httpx.Client(transport=httpx.MockTransport(lambda req: calls.append(req) or httpx.Response(503)))
    try:
        list(producer.wikimedia_records(http, max_retries=3, sleep=no_sleep))
        raise AssertionError("expected FeedUnavailable")
    except producer.FeedUnavailable:
        pass
    assert len(calls) == 3


def test_user_agent_has_no_email():
    requests = []
    http = httpx.Client(transport=httpx.MockTransport(
        lambda req: requests.append(req) or httpx.Response(200, content=sse(("1", edit("a"))))
    ))
    producer.run(ListSink(), "wikimedia", max_events=1, http=http, sleep=no_sleep)
    assert requests[0].headers["user-agent"] == producer.USER_AGENT
    assert "@" not in producer.USER_AGENT
    assert "github.com/wilkinson001/de-playground" in producer.USER_AGENT


def test_empty_streams_back_off_and_give_up():
    # A feed that keeps answering 200 but closing without events must not be hammered: each empty
    # connection counts as a failure (with backoff), so it eventually falls back.
    calls, sleeps = [], []

    def handler(request):
        calls.append(request)
        if len(calls) > 10:
            raise RuntimeError("reconnecting in a tight loop")
        return httpx.Response(200, content=b"")

    http = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        list(producer.wikimedia_records(http, max_retries=3, backoff_s=2.0, sleep=sleeps.append))
        raise AssertionError("expected FeedUnavailable")
    except producer.FeedUnavailable:
        pass
    assert len(calls) == 3
    assert sleeps == [2.0, 4.0]
