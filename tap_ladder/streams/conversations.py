"""Conversations stream: one row per recorded in-home visit."""

from __future__ import annotations

import sys
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from singer_sdk import typing as th
from singer_sdk.pagination import JSONPathPaginator

from tap_ladder.client import LadderStream

if sys.version_info >= (3, 12):
    from typing import override
else:  # pragma: no cover
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context, Record

PAGE_SIZE = 100  # API max

_NULLABLE_STRING = th.StringType(nullable=True)
_NULLABLE_DATETIME = th.DateTimeType(nullable=True)

_ADDRESS = th.ObjectType(
    th.Property("name", _NULLABLE_STRING),
    th.Property("street", _NULLABLE_STRING),
    th.Property("street2", _NULLABLE_STRING),
    th.Property("city", _NULLABLE_STRING),
    th.Property("state", _NULLABLE_STRING),
    th.Property("postal_code", _NULLABLE_STRING),
    th.Property("country", _NULLABLE_STRING),
    th.Property("latitude", th.NumberType(nullable=True)),
    th.Property("longitude", th.NumberType(nullable=True)),
    nullable=True,
)

# ``url`` / ``expires_at`` are deliberately absent from both file blocks: they are
# presigned links that expire ~1 hour after listing and grant access to customer
# audio. Ladder says not to store them, so ``post_process`` strips them.
_TRANSCRIPT = th.ObjectType(
    th.Property("status", _NULLABLE_STRING),
    th.Property("completed_at", _NULLABLE_DATETIME),
    th.Property("languages", th.ArrayType(th.StringType, nullable=True)),
    nullable=True,
)

_AUDIO = th.ObjectType(
    th.Property("status", _NULLABLE_STRING),
    th.Property("duration_seconds", th.NumberType(nullable=True)),
    nullable=True,
)


class ConversationsStream(LadderStream):
    """``GET /api/v1/conversations``, incremental on ``updated_at``.

    ``updated_at`` moves when the conversation *or its final transcript* changes,
    so a transcript finishing re-emits the row and re-triggers the transcripts
    child stream. Rep / CRM / customer / address fields are the capture-time
    snapshot and do not follow later CRM edits.
    """

    name = "conversations"
    path = "/api/v1/conversations"
    records_jsonpath = "$.items[*]"
    primary_keys = ("conversation_id",)
    replication_key = "updated_at"
    # The API lists newest-first by recording, not by ``updated_at``, so the SDK
    # must track the max and only commit the bookmark at the end of the sync.
    is_sorted = False

    schema = th.PropertiesList(
        th.Property("conversation_id", th.StringType, description="Ladder conversation id"),
        th.Property("status", _NULLABLE_STRING, description="active | ended | complete"),
        th.Property("rep_name", _NULLABLE_STRING),
        th.Property("crm_project_id", _NULLABLE_STRING),
        th.Property("crm_job_id", _NULLABLE_STRING),
        th.Property("crm_appointment_id", _NULLABLE_STRING),
        th.Property("crm_customer_id", _NULLABLE_STRING),
        th.Property("customer_name", _NULLABLE_STRING),
        th.Property("location_name", _NULLABLE_STRING),
        th.Property("address", _ADDRESS),
        th.Property("started_at", _NULLABLE_DATETIME),
        th.Property("ended_at", _NULLABLE_DATETIME),
        th.Property("created_at", _NULLABLE_DATETIME),
        th.Property("updated_at", _NULLABLE_DATETIME),
        th.Property("transcript", _TRANSCRIPT),
        th.Property("audio", _AUDIO),
    ).to_dict()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Init the stream with an empty transcript-link handoff buffer."""
        super().__init__(*args, **kwargs)
        # conversation_id -> presigned transcript URL, consumed by the child stream.
        self.transcript_urls: dict[str, str] = {}

    @override
    def get_new_paginator(self) -> JSONPathPaginator:
        """Follow ``next_page`` as ``cursor`` until it is null."""
        return JSONPathPaginator("$.next_page")

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return query params for the request.

        The cursor only encodes a position in the result set, so the
        ``updated_after`` filter is sent on every page, not just the first.
        """
        params: dict[str, Any] = {"size": PAGE_SIZE}
        start = self.get_starting_timestamp(context)
        if start:
            start -= timedelta(minutes=self.config.get("lookback_minutes") or 0)
            params["updated_after"] = start.isoformat()
        if next_page_token:
            params["cursor"] = next_page_token
        return params

    @override
    def post_process(self, row: Record, context: Context | None = None) -> Record | None:
        """Drop null-PK rows, hand transcript links to the child, strip all links."""
        row = super().post_process(row, context)
        if row is None:
            return None

        transcript = row.get("transcript")
        if (
            transcript
            and transcript.get("status") == "ready"
            and transcript.get("url")
            # Only buffer links a selected child will consume.
            and any(child.selected for child in self.child_streams)
        ):
            self.transcript_urls[row["conversation_id"]] = transcript["url"]

        for block in (transcript, row.get("audio")):
            if block:
                block.pop("url", None)
                block.pop("expires_at", None)
        return row

    @override
    def generate_child_contexts(
        self,
        record: Record,
        context: Context | None,
    ) -> Iterable[Context | None]:
        """Sync the transcript only when a downloadable one was just listed."""
        if record["conversation_id"] not in self.transcript_urls:
            return
        transcript = record["transcript"]
        yield {
            "conversation_id": record["conversation_id"],
            "conversation_updated_at": record.get("updated_at"),
            "transcript_completed_at": transcript.get("completed_at"),
            "languages": transcript.get("languages"),
        }
