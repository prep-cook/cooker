Changed

- `POST /v1/responses` and `/v1/responses/compact`, and their `/backend-api/codex/` paths, check the request before any upstream is contacted: a malformed request answers 400 (413 when too large) and nothing is sent upstream.
- Every Responses answer carries a response id cooker issued, the same across failover and resume. A later request continues it with `previous_response_id` whichever account serves either turn: cooker resends the stored conversation, and the upstream receives no `previous_response_id`. An id cooker did not issue, or one from another client key, answers 400 with code `previous_response_not_found`.
- A Responses request naming `conversation` answers 400 with code `unsupported_parameter`.
- The Responses routes send nothing before the first byte of the answer. `requests.nonstream-keepalive-interval`, `requests.streaming.keepalive-seconds` and `requests.streaming.bootstrap-retries` no longer apply to them; a failure before that byte moves to the next credential instead. Streamed answers carry `X-Ck-Stream-Id`.
- A client that leaves a Responses answer after the upstream started answering no longer cancels the upstream request: the request runs to its end and its usage record keeps the final token counts.
- A Responses request is not sent to an account only the legacy path serves: Gemini Interactions keys, Devin, AI Studio, and a Meta request with a custom `apply_patch` tool. With no other candidate it answers 400 with "this account does not serve the Responses API". Force-mapped aliases and Claude accounts eligible for thinking replay now serve Responses requests, and the answer shows the alias as its `model`.
- A Claude `/v1/responses/compact` returns Anthropic's signed compaction block inside a `compaction` item's `encrypted_content`. Sending that item back puts the block first in the next Claude request and leaves out the messages before it; a malformed carried block answers 400.
- A Responses request whose `prompt` has no string `id` answers 400 "invalid protocol schema" instead of "prompt.id must be a string".
- A Responses answer whose upstream reported no usage now writes a usage record without token counts instead of none.
