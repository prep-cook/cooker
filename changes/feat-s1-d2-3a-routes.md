Changed

- Chat Completions, Anthropic Messages and Gemini requests are checked before any upstream is contacted: a malformed request answers 400 (413 when too large) in the route's error format, and nothing is sent upstream.
- These routes send nothing before the first byte of the answer. `requests.nonstream-keepalive-interval`, `requests.streaming.keepalive-seconds` and `requests.streaming.bootstrap-retries` no longer apply to them; a failure before that byte moves to the next credential instead. Streamed answers carry `X-Ck-Stream-Id` and a resume cursor on each event.
- A Responses-shaped body sent to `/v1/chat/completions` is refused with 400 instead of being converted.
- A request bound for an Anthropic upstream without `max_tokens` is no longer given 1024: the `model-catalog` `default-max-tokens` or the model's registered output limit applies, and a candidate with neither is skipped. This holds for force-mapped aliases too, and Chat and Gemini requests no longer get 32000 or 64000.
- `POST /v1/completions` answers 501 with code `unsupported_endpoint`, and `POST /v1beta/interactions` answers 501 in the Interactions error shape with code `unimplemented`.
- Usage records from Gemini and Vertex upstreams count reasoning inside `output_tokens`, and a count the upstream did not report is absent instead of 0.
- A Gemini `MALFORMED_FUNCTION_CALL` ends an Anthropic answer as a normal turn instead of a 502.
