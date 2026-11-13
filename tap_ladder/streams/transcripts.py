"""Transcripts stream: the final transcript JSON for each conversation."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from singer_sdk import typing as th
from singer_sdk.streams import RESTStream

from tap_ladder.streams.conversations import ConversationsStream

if sys.version_info >= (3, 12):
    from typing import override
else:  # pragma: no cover
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Context, Record


class TranscriptsStream(RESTStream):
    """Download each ready transcript from its presigned link.

    A child of ``conversations``: it only runs for conversations listed in this
    sync with a ``ready`` transcript, i.e. new transcripts and ones Ladder
    re-finalized (``updated_at`` covers both). The loader MERGEs on
    ``conversation_id``, so a re-run replaces rather than duplicates.

    This deliberately subclasses ``RESTStream`` rather than ``LadderStream``: the
    request goes to Ladder's file storage, not the API, and must not carry the
    ``Api-Key`` header.
    """

    name = "transcripts"
    parent_stream_type = ConversationsStream
    primary_keys = ("conversation_id",)
    # One bookmark for the stream, not one state entry per conversation.
    state_partitioning_keys = ()
    url_base = ""
    # Never requested (``get_url`` returns the presigned link); the SDK uses it as
    # the endpoint label in request metrics, which keeps the signed URL out of logs.
    path = "/transcript-file"

    schema = th.PropertiesList(
        th.Property("conversation_id", th.StringType),
        th.Property(
            "conversation_updated_at",
            th.DateTimeType(nullable=True),
            description="Parent conversation's updated_at when this transcript was pulled",
        ),
        th.Property("transcript_completed_at", th.DateTimeType(nullable=True)),
        th.Property("languages", th.ArrayType(th.StringType, nullable=True)),
        th.Property(
            "transcript_bytes",
            th.IntegerType(nullable=True),
            description="Size of the downloaded transcript file",
        ),
        # Shape is not part of Ladder's published contract, so it is not
        # enumerated: an object/array with no ``properties`` passes through the
        # SDK untouched and lands as a VARIANT. Parse it in dbt.
        th.Property(
            "transcript",
            th.CustomType({"type": ["object", "array", "null"]}),
            description="Final transcript JSON as Ladder serves it; null if over max size",
        ),
    ).to_dict()

    @property
    def _parent(self) -> ConversationsStream:
        return self._tap.streams[ConversationsStream.name]  # type: ignore[return-value]

    @override
    def get_url(self, context: Context | None) -> str:
        """Return (and consume) the presigned link the parent just listed."""
        assert context is not None  # noqa: S101 - always a child-context sync
        return self._parent.transcript_urls.pop(context["conversation_id"])

    @override
    def parse_response(self, response: requests.Response) -> Iterable[Record]:
        """Yield the whole file as a single record."""
        yield {"transcript_bytes": len(response.content), "transcript": response.json()}

    @override
    def post_process(self, row: Record, context: Context | None = None) -> Record | None:
        """Attach the parent's ids and null out bodies too large for a VARIANT."""
        assert context is not None  # noqa: S101 - always a child-context sync
        max_bytes = self.config.get("max_transcript_bytes")
        if max_bytes and row["transcript_bytes"] > max_bytes:
            self.logger.warning(
                "transcripts: %s is %d bytes (> max_transcript_bytes=%d); landing without body",
                context["conversation_id"],
                row["transcript_bytes"],
                max_bytes,
            )
            row["transcript"] = None
        return {**context, **row}
