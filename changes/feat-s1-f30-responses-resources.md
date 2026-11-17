Added

- `GET /v1/responses/{id}`, `POST /v1/responses/{id}/cancel` and `DELETE /v1/responses/{id}` serve the caller's stored Responses turns from local state. Another caller's id and an unknown id get the same 404, and a deleted response can no longer be continued with `previous_response_id`.
- A Responses request with `background: true` and `stream: false` answers at once with a queued response; `GET /v1/responses/{id}` shows it in progress, then completed, failed or cancelled. A streamed `background: true` request is delivered live and retrieved the same way. `background` with `store: false` is refused with 400.
- `background` is no longer sent upstream: Meta no longer receives the forbidden `background: true` with `stream: true`, and Chat, Anthropic and Gemini upstreams no longer refuse a request that names `background`, including a retried or continued one.
