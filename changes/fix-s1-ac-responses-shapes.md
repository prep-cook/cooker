Fixed

- Include required nullable fields, request instructions, metadata and sampling settings in Chat-backed Responses output, and emit `response.in_progress` immediately after `response.created` without duplicating native lifecycle events.
- Include `stop_sequence: null` in translated Anthropic messages and terminal deltas while preserving native stop sequences.
