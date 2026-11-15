Security

- Codex reasoning replay is kept per client key and per upstream credential, so a turn cached for one caller or credential is never sent with another's request that reuses the same session id.
- Codex credentials run on the execution contract over HTTP: every client protocol is encoded by the Responses request codec and shaped with the Codex backend's rules, generation always streams from `/responses`, compact uses the native `/responses/compact`, and token counts stay local.
- A local Codex token count includes message text sent as one string.
- A request that turns thinking off (Anthropic `thinking: {"type":"disabled"}`, Gemini `thinkingBudget: 0`) reaches Responses and Codex upstreams as `"reasoning":{"effort":"none"}` instead of the upstream default; a model without that setting returns the upstream's own error.
- A Codex stream with invalid UTF-8 fails as an upstream error instead of passing repaired text, and a stream that ends before its completion event finishes fails as an incomplete response (408).
