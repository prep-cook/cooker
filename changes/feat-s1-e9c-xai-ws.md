Changed

- Responses WebSocket turns on an xAI credential with `websockets` go over HTTP until the new upstream socket transport is wired into the server; continuations then replay through the gateway's own history, as for every other HTTP provider.

Internal

- The xAI upstream WebSocket implements the duplex `Turns` contract: each turn sends its expanded request as one `response.create`, hands every upstream frame to the ingress unchanged, keeps no transcript or response-id map, and treats a lost socket, including xAI's 59-minute connection end, as the attempt's failure without reconnecting.
