import json

from de_playground.streaming.events import RECORD_FIELDS, SseEvent, decode, parse_sse, to_record

EDIT = {
    "$schema": "/mediawiki/recentchange/1.0.0",
    "meta": {
        "uri": "https://en.wikipedia.org/wiki/Example",
        "id": "0b6f5c0e-1a2b-4c3d-9e8f-001122334455",
        "dt": "2026-11-11T10:00:05.123Z",
        "domain": "en.wikipedia.org",
        "stream": "mediawiki.recentchange",
    },
    "id": 1234567,
    "type": "edit",
    "namespace": 0,
    "title": "Example",
    "comment": "copyedit",
    "timestamp": 1794391205,
    "user": "Alice",
    "bot": False,
    "minor": True,
    "length": {"old": 1000, "new": 1150},
    "revision": {"old": 1, "new": 2},
    "server_name": "en.wikipedia.org",
    "wiki": "enwiki",
}


def test_parse_sse_multiline_ids_and_comments():
    lines = [
        ":heartbeat",
        "event: message",
        "id: x1",
        "data: a",
        "data: b",
        "",
        "id: x2",
        "data: never-terminated",
    ]
    assert list(parse_sse(lines)) == [SseEvent("x1", "a\nb")]


def test_to_record_edit():
    assert to_record(EDIT) == {
        "event_id": "0b6f5c0e-1a2b-4c3d-9e8f-001122334455",
        "event_time": "2026-11-11T10:00:05.123Z",
        "wiki": "enwiki",
        "server_name": "en.wikipedia.org",
        "type": "edit",
        "namespace": 0,
        "title": "Example",
        "user": "Alice",
        "bot": False,
        "minor": True,
        "length_old": 1000,
        "length_new": 1150,
        "byte_delta": 150,
    }
    assert list(to_record(EDIT)) == RECORD_FIELDS


def test_event_time_normalised_to_utc_millis():
    for dt in ("2026-11-11T10:00:05Z", "2026-11-11T10:00:05+00:00", "2026-11-11T11:00:05+01:00"):
        raw = {**EDIT, "meta": {**EDIT["meta"], "dt": dt}}
        assert to_record(raw)["event_time"] == "2026-11-11T10:00:05.000Z"


def test_to_record_new_page_without_old_length():
    raw = {**EDIT, "type": "new", "length": {"new": 300}}
    rec = to_record(raw)
    assert (rec["length_old"], rec["length_new"], rec["byte_delta"]) == (None, 300, 300)


def test_to_record_log_event_without_lengths_or_minor():
    raw = {k: v for k, v in EDIT.items() if k not in ("length", "minor")}
    rec = to_record({**raw, "type": "log"})
    assert (rec["length_old"], rec["length_new"], rec["byte_delta"], rec["minor"]) == (None, None, 0, False)


def test_canary_and_malformed_skipped():
    canary = {**EDIT, "meta": {**EDIT["meta"], "domain": "canary"}}
    assert to_record(canary) is None
    assert to_record({**EDIT, "meta": {k: v for k, v in EDIT["meta"].items() if k != "dt"}}) is None
    assert to_record({k: v for k, v in EDIT.items() if k != "wiki"}) is None
    assert decode(SseEvent(None, "{not json")) is None
    assert decode(SseEvent("x", json.dumps(EDIT)))["wiki"] == "enwiki"
