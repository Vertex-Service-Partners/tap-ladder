"""Tests for tap-ladder.

The SDK harness (``get_tap_test_class``) runs discovery + record-emission tests
against the live API, so it only runs when ``TAP_LADDER_API_KEY`` is set. The
tests below run with no credentials and pin the decisions that are easy to
"clean up" and silently regress.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from singer_sdk.testing import get_tap_test_class

from tap_ladder.client import LadderStream
from tap_ladder.streams import ConversationsStream, TranscriptsStream
from tap_ladder.tap import TapLadder

API_KEY = os.environ.get("TAP_LADDER_API_KEY")

if API_KEY:
    TestTapLadder = get_tap_test_class(
        tap_class=TapLadder,
        # Bound the live sync so the harness doesn't pull the full history.
        config={
            "api_key": API_KEY,
            "start_date": os.environ.get("TAP_LADDER_START_DATE", "2026-11-01T00:00:00Z"),
        },
    )


class _FakeResponse:
    """Minimal stand-in for requests.Response."""

    def __init__(self, payload: object) -> None:
        self.content = json.dumps(payload).encode()
        self._payload = payload

    def json(self) -> object:
        return self._payload


def _tap(**config: object) -> TapLadder:
    return TapLadder(config={"api_key": "x", **config}, validate_config=False)


def _record(**overrides: object) -> dict:
    return {
        "conversation_id": "c1",
        "updated_at": "2026-11-01T17:20:00Z",
        "transcript": {
            "status": "ready",
            "completed_at": "2026-11-01T17:20:00Z",
            "languages": ["en"],
            "url": "https://files.example/t.json?sig=secret",
            "expires_at": "2026-11-01T18:20:00Z",
        },
        "audio": {
            "status": "ready",
            "duration_seconds": 3595.0,
            "url": "https://files.example/a.mp3?sig=secret",
            "expires_at": "2026-11-01T18:20:00Z",
        },
        **overrides,
    }


def test_pk_is_not_required() -> None:
    """PK must not be required=True — target-snowflake would make it NOT NULL."""
    assert "conversation_id" not in ConversationsStream.schema.get("required", [])
    assert "conversation_id" not in TranscriptsStream.schema.get("required", [])


def test_presigned_links_are_not_in_schema() -> None:
    """Ladder's file links expire in ~1h and grant file access; never land them."""
    props = ConversationsStream.schema["properties"]
    for block in ("transcript", "audio"):
        assert "url" not in props[block]["properties"]
        assert "expires_at" not in props[block]["properties"]


def test_address_sub_fields_are_enumerated() -> None:
    """An ObjectType with no children silently drops every sub-field."""
    address = ConversationsStream.schema["properties"]["address"]["properties"]
    assert {"street", "city", "state", "postal_code", "latitude", "longitude"} <= set(address)


def test_post_process_strips_links_and_hands_off_transcript_url() -> None:
    tap = _tap()
    stream = tap.streams["conversations"]
    row = stream.post_process(_record())
    assert row is not None
    assert "url" not in row["transcript"]
    assert "url" not in row["audio"]
    assert "expires_at" not in row["audio"]
    assert stream.transcript_urls == {"c1": "https://files.example/t.json?sig=secret"}

    contexts = list(stream.generate_child_contexts(row, None))
    assert contexts == [
        {
            "conversation_id": "c1",
            "conversation_updated_at": "2026-11-01T17:20:00Z",
            "transcript_completed_at": "2026-11-01T17:20:00Z",
            "languages": ["en"],
        },
    ]
    # The child consumes the link exactly once.
    child = tap.streams["transcripts"]
    assert child.get_url(contexts[0]) == "https://files.example/t.json?sig=secret"
    assert stream.transcript_urls == {}


def test_no_child_sync_when_transcript_not_ready() -> None:
    stream = _tap().streams["conversations"]
    for transcript in (None, {"status": "processing"}, {"status": "failed"}):
        row = stream.post_process(_record(transcript=transcript))
        assert row is not None
        assert list(stream.generate_child_contexts(row, None)) == []
    assert stream.transcript_urls == {}


def test_post_process_drops_null_pk() -> None:
    stream = _tap().streams["conversations"]
    assert stream.post_process(_record(conversation_id=None)) is None
    assert stream.post_process(_record(conversation_id="")) is None
    assert stream.transcript_urls == {}


def test_url_params_resend_filter_with_cursor_and_apply_lookback() -> None:
    """The cursor is only a position; updated_after must ride on every page."""
    stream = _tap(start_date="2026-11-01T00:00:00+00:00", lookback_minutes=15).streams[
        "conversations"
    ]
    stream._write_starting_replication_value(None)  # what sync() does first
    first = stream.get_url_params(None, None)
    assert first == {"size": 100, "updated_after": "2026-10-31T23:45:00+00:00"}
    nxt = stream.get_url_params(None, "abc")
    assert nxt == {**first, "cursor": "abc"}
    assert datetime.fromisoformat(first["updated_after"]).tzinfo == timezone.utc


def test_url_params_without_start_pull_everything() -> None:
    stream = _tap().streams["conversations"]
    assert stream.get_url_params(None, None) == {"size": 100}


def test_transcript_record_merges_context_and_keeps_body() -> None:
    child = _tap().streams["transcripts"]
    body = {"segments": [{"speaker": "rep", "text": "hi"}]}
    (raw,) = child.parse_response(_FakeResponse(body))
    row = child.post_process(raw, {"conversation_id": "c1", "languages": ["en"]})
    assert row is not None
    assert row["conversation_id"] == "c1"
    assert row["transcript"] == body
    assert row["transcript_bytes"] == len(json.dumps(body))


def test_oversized_transcript_lands_without_body() -> None:
    child = _tap(max_transcript_bytes=10).streams["transcripts"]
    (raw,) = child.parse_response(_FakeResponse({"segments": ["x" * 50]}))
    row = child.post_process(raw, {"conversation_id": "c1"})
    assert row is not None
    assert row["transcript"] is None
    assert row["transcript_bytes"] > 10


def test_transcript_requests_do_not_send_api_key() -> None:
    """The file host is not Ladder's API; the key must not leave for it."""
    child = _tap().streams["transcripts"]
    assert not isinstance(child, LadderStream)
    assert "Api-Key" not in child.http_headers
    assert "Api-Key" not in getattr(child.authenticator, "auth_headers", {})
