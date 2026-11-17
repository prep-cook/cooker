Fixed

- A Responses WebSocket turn on a Codex credential with `websockets` completes over HTTP, with its full conversation, when the upstream refuses the WebSocket upgrade with 426, instead of failing on that credential.
