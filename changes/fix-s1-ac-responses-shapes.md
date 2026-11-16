Fixed

- Include required nullable fields, request instructions, metadata and sampling settings in Chat-backed Responses output for buffered, streamed and resumed delivery; leave compact envelopes unchanged. Emit `response.in_progress` immediately after `response.created` without duplicating native lifecycle events.
- Include `stop_sequence: null` in translated Anthropic messages and terminal deltas while preserving native stop sequences.
