Fixed

- Route Responses requests with a reusable `prompt` and no `model` only to native OpenAI/Codex credentials, leaving model resolution upstream. Return a Responses-shaped 400 saying model is required when no native resolver is available; explicit-model routing is unchanged.
