Internal

- The Claude executor runs under the execution contract: every client protocol encodes to the Messages API through the Anthropic codec, the response decodes into canonical events, and counts away from api.anthropic.com use the local tokenizer. The old route stays for the server until D2 and for Kimi until P8.
- The Anthropic fence refuses a thinking budget below the model's minimum, a budget sent to an adaptive-only model, and a forced tool with manual thinking, instead of adjusting the request. On the API-key path the executor sends the client's `temperature`, `top_p`, thinking, effort and `anthropic-beta` unchanged; requests the OAuth cloak shapes go upstream as on the old route.
- `message_delta.usage` carries only `MessageDeltaUsage` members; `cache_creation` and `service_tier` stay in `message_start`.
- `Client::usage` carries request-side facts only (contract amendment G4).
