Added

- `POST /v1/responses/input_tokens` answers `{"object":"response.input_tokens","input_tokens":N}` with the upstream's own count, including the stored history a `previous_response_id` names. Only a destination with a token-counting endpoint serves it (Claude with an API key on Anthropic's origin, Gemini and Vertex Gemini); an account that could only estimate the count locally is skipped, and the request is refused with `this account has no token-counting endpoint` when no candidate can count.
