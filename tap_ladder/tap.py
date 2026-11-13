"""Ladder tap class."""

from __future__ import annotations

from singer_sdk import RESTStream, Tap
from singer_sdk import typing as th

from tap_ladder import streams


class TapLadder(Tap):
    """Singer tap for the Ladder Data API."""

    name = "tap-ladder"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description="Ladder Data API key, sent in the `Api-Key` header. Scoped to one org.",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://app.ladderai.com",
            description="Base URL for the Ladder Data API.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description=(
                "Earliest `updated_at` to sync on the first run (ISO 8601, tz-aware). "
                "Omit to pull the full history."
            ),
        ),
        th.Property(
            "lookback_minutes",
            th.IntegerType,
            default=15,
            description=(
                "Minutes subtracted from the bookmark on each incremental run, so a row "
                "committed just behind the previous run's high-water mark is not missed. "
                "Re-pulled rows are deduped by the loader's MERGE on conversation_id."
            ),
        ),
        th.Property(
            "max_transcript_bytes",
            th.IntegerType,
            default=15_000_000,
            description=(
                "Transcript files larger than this are emitted with a null body (and their "
                "size) instead of failing the load; Snowflake VARIANT caps at 16 MB."
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[RESTStream]:
        """Return a list of discovered streams.

        Returns:
            A list of the tap's streams.
        """
        return [
            streams.ConversationsStream(self),
            streams.TranscriptsStream(self),
        ]


if __name__ == "__main__":
    TapLadder.cli()
