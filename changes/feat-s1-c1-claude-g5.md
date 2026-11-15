Changed

- A `/v1/responses/compact` request, or a Responses request carrying a `compaction_trigger` item, routed to a Claude credential now fails with 400 before any upstream call. cooker no longer writes a summary of its own in place of a native compaction.
