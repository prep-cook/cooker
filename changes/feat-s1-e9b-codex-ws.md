Internal

- The Codex executor runs Responses WebSocket turns on the execution contract's duplex session. Each turn sends its expanded history as one `response.create` with the same handshake headers and frame members as before, every upstream response is its own attempt stream, and steering and interrupt frames reach the turn's socket unchanged. Cancel interrupts the response in flight and closes the socket. The server's WebSocket handler still uses the old turn path.
