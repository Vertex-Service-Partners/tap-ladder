# tap-ladder

A [Singer](https://www.singer.io/) tap for the [Ladder](https://ladderai.com) Data
API, built with the [Meltano Singer SDK](https://sdk.meltano.com).

Ladder records in-home sales conversations. This tap lands each conversation's
metadata and its final transcript in the Vertex warehouse.

## Streams

| Stream          | Source                                      | Replication                 | Primary key       |
|-----------------|---------------------------------------------|-----------------------------|-------------------|
| `conversations` | `GET /api/v1/conversations`                 | incremental on `updated_at` | `conversation_id` |
| `transcripts`   | presigned `transcript.url` per conversation | child of `conversations`    | `conversation_id` |

* `conversations.updated_at` moves when the conversation **or its final
  transcript** changes, so a transcript finishing re-emits the conversation and
  re-downloads the transcript. The loader MERGEs on `conversation_id`.
* `transcripts` only runs for conversations whose transcript is `ready` in this
  sync. The body is landed as-is in a single `transcript` column (VARIANT); its
  shape is not part of Ladder's published contract, so parse it in dbt. Files
  over `max_transcript_bytes` land with a null body and their size.
* **Audio is not ingested.** The merged recording is a binary file; only its
  `status` and `duration_seconds` are kept.
* **Presigned links are never landed.** `transcript.url`, `audio.url` and their
  `expires_at` expire ~1 hour after listing and grant file access; the tap strips
  them, and the transcript download does not send the `Api-Key` header.
* Rep / CRM / customer / address fields are the capture-time snapshot and do not
  follow later CRM edits. They contain customer PII, as do transcripts.

## Configuration

| Setting                | Required | Default                    | Description |
|------------------------|----------|----------------------------|-------------|
| `api_key`              | yes      | —                          | Org-scoped key, sent in the `Api-Key` header (secret). |
| `api_url`              | no       | `https://app.ladderai.com` | API base URL. |
| `start_date`           | no       | —                          | Earliest `updated_at` on the first run (tz-aware ISO 8601). Omit for full history. |
| `lookback_minutes`     | no       | `15`                       | Subtracted from the bookmark each run so late-committed rows aren't missed. |
| `max_transcript_bytes` | no       | `15000000`                 | Larger transcripts land without a body (Snowflake VARIANT caps at 16 MB). |

Provide the key via the environment so it never lands on disk:

```bash
export TAP_LADDER_API_KEY='...'
```

## API notes

* **Pagination:** cursor (fastapi-pagination `CursorPage`) — pass `next_page` back
  as `cursor` until null. The cursor is only a position, so filters are re-sent on
  every page. Page size max 100.
* **Ordering:** newest recording first, *not* by `updated_at`, so the stream is
  unsorted and the bookmark is committed only at the end of a successful sync.
* **Errors:** 401 `missing_api_key` / `invalid_api_key`; 422 for a bad parameter
  (e.g. a datetime without a timezone). 429/5xx are retried with backoff.

## Develop

```bash
uv venv && uv pip install --python .venv/bin/python -e . pytest 'singer-sdk[testing]'

.venv/bin/tap-ladder --about
.venv/bin/tap-ladder --discover --config ENV     # ENV reads TAP_LADDER_* vars
.venv/bin/tap-ladder --config ENV > out.jsonl 2> err.log

# Tests (the live-API harness runs only when TAP_LADDER_API_KEY is set)
.venv/bin/python -m pytest
```

Or via Meltano:

```bash
uvx meltano install
uvx meltano run tap-ladder target-jsonl
```
