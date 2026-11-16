Internal

- The Codex executor serves Alpha Search through the execution contract: a credential that may not search is refused before any send, so the next candidate is tried, and the result streams through the search codec into one `operation` record. The `/v1/alpha/search` route still uses the old path until the media routes move.
