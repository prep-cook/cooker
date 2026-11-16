Changed

- Responses WebSocket turns on an xAI credential with `websockets` go over HTTP until the new upstream socket transport is wired into the server; continuations then replay through the gateway's own history, as for every other HTTP provider.
- An xAI response whose upstream fails while an `apply_patch` call is open now ends with the apply_patch 502, as when the body ends early, instead of the upstream's own failure status.
- The xAI upstream WebSocket implements the duplex `Turns` contract: each turn sends its expanded request as one `response.create` with `store: false`, hands every upstream frame to the ingress unchanged, keeps no transcript or response-id map, and treats a lost socket, including xAI's 59-minute connection end, as the attempt's failure without reconnecting.
- On that socket, xAI's `response.done` ends a response like `response.completed`, and an incomplete response's usage is recorded, as on the HTTP path.
