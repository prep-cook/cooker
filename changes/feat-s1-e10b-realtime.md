Changed

- Realtime WebSockets and call sidebands now check each client event before forwarding it. An unknown event type, a binary message or a malformed event gets a Realtime `error` event and never reaches the upstream; every accepted message is still forwarded byte for byte.
- Realtime sockets record usage: each response's `response.done` usage and each input audio transcription's token usage is its own usage record, beside one record for the session.
- Realtime client sockets are pinged every 30 seconds and closed after 60 seconds without a pong or a write blocked for 10 seconds. `realtime.ping-interval-seconds`, `realtime.read-timeout-seconds`, `realtime.write-timeout-seconds` and `realtime.max-sockets-per-key` change these at startup.
- Every Realtime socket end writes one `live_session_end` log line naming its cause (`client_close`, `client_gone`, `client_read_timeout`, `client_write_timeout`, `upstream_close`, `upstream_gone`, `hangup`, `socket_limit`, `shutdown` or `relay_error`).
- When a Realtime client disconnects without a close frame, the upstream session is closed with code 1001 (going away).
