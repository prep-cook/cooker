Changed

- WebRTC calls (`POST /v1/realtime/calls`, `/v1/realtime` and `/v1/live`) always go through the media relay. A server without it, because the build lacks the `media-relay` feature or `oauth.providers.codex.live-media-relay.enabled` is false, answers 501 instead of letting media flow directly between the client and the upstream.
- The Docker image includes the WebRTC media relay.
- A relayed call's `oai-events` data channel is checked like a Realtime WebSocket: an unknown event type, a binary message, a malformed event or a `session.update` naming another model gets a Realtime `error` event and never reaches the upstream; every accepted message is forwarded unchanged.
- A relayed call records each response's `response.done` usage as its own usage record, beside one record for the call.
- A call's sideband WebSocket records no usage; the call's data channel records each response once.
- Every relayed call writes one `live_session_end` log line naming its cause, with the RTP packets and payload bytes relayed in each direction. When the client or the upstream closes its data channel or loses its connection, the call ends and both media connections close.
