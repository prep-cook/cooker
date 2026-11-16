Fixed

- The Anthropic dialect of `GET /v1/models` pages the catalog with `after_id`, `before_id` and `limit` (default 20, 1–1000), returns accurate `first_id`, `last_id` and `has_more`, and answers an unknown cursor or out-of-range limit with `invalid_request_error` 400.
- Every Anthropic model entry now carries `created_at`, using `1970-01-01T00:00:00Z` when the release date is unknown, and an empty page reports `first_id` and `last_id` as null.
