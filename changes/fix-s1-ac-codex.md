Fixed

- Codex requests retain prompt cache options, output encryption inclusion, explicit parallel-tool disabling, and reasoning-summary delivery options when the client buffers the response, including Sign in with ChatGPT requests.
- Codex HTTP and WebSocket responses no longer treat `response.done` as successful completion. Incomplete responses retain their wire status and do not populate reasoning replay, including duplex steering.
- Codex reasoning-summary delivery remains in a Codex-only provider map. Public Responses prompt cache options remain supported.
