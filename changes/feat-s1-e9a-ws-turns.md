Changed

- Each Responses WebSocket `response.create` runs as one Responses turn with cooker's response id. `previous_response_id` continues a stored turn, or the socket's last `store: false` turn on that socket only; every attempt resends the history, so a continuation can move to another account. Unknown ids get a `previous_response_not_found` error frame without an upstream call.
- A Responses WebSocket turn that fails before it starts gets an `error` frame, one that fails after gets `response.failed`, and the socket stays open for the next turn. Closing the socket cancels the turn in flight upstream.
- While a Responses WebSocket turn runs, one more `response.create` waits; a further one gets a `working_set_unavailable` error frame.
- A stored Responses turn shows `store: true`.
- The new `responses.ignore-client-store-false` setting stores every Responses turn, so a Codex client that reconnects can continue its conversation.
- The Responses WebSocket no longer keeps a per-connection transcript, closes with 1012 to ask for an HTTP replay, steers responses or answers `generate: false` warm-ups locally.
