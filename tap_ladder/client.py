"""REST client handling, including LadderStream base class.

The Ladder Data API (https://app.ladderai.com) is a single read endpoint:

* Auth is one org-scoped API key in the ``Api-Key`` header. The key decides the
  organization; no request names one.
* ``GET /api/v1/conversations`` returns ``{"items": [...], "next_page": ...}``.
  Paging is cursor-based (fastapi-pagination ``CursorPage``): pass ``next_page``
  back as ``cursor`` until it is null. The cursor only encodes a position, so the
  filters are re-sent on every page.
* Errors: 401 ``missing_api_key`` / ``invalid_api_key``, 422 for a bad parameter
  (e.g. a datetime without a timezone). Neither is retriable.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:  # pragma: no cover
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record


class LadderStream(RESTStream):
    """Ladder base stream class."""

    @property
    @override
    def url_base(self) -> str:
        """Return the API root, configurable via the ``api_url`` setting."""
        return self.config["api_url"].rstrip("/")

    @property
    @override
    def authenticator(self) -> APIKeyAuthenticator:
        """Send the org's key in the ``Api-Key`` header on every request."""
        return APIKeyAuthenticator(key="Api-Key", value=self.config["api_key"], location="header")

    @property
    @override
    def http_headers(self) -> dict[str, str]:
        """HTTP headers for each request."""
        headers = super().http_headers
        headers["Accept"] = "application/json"
        return headers

    @override
    def backoff_max_tries(self) -> int:
        """Retry 429/5xx a few more times than the SDK default."""
        return 8

    @override
    def post_process(self, row: Record, context: Context | None = None) -> Record | None:
        """Drop records with a null/empty primary key.

        A null PK breaks ``target-snowflake``'s MERGE (``NULL = NULL`` is false,
        so it inserts an unmergeable duplicate every run). Subclasses that need
        extra shaping should call ``super().post_process(...)`` first.
        """
        del context
        for key in self.primary_keys or ():
            if not row.get(key):
                self.logger.warning("%s: dropping record with null/empty PK %r", self.name, key)
                return None
        return row
