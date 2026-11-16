Fixed

- Route Responses requests with a reusable `prompt` and no `model` only to native OpenAI/Codex credentials, leaving model resolution upstream. Return a Responses-shaped 400 saying model is required when no native resolver exists; cooling credentials retain their retryable errors. Reject prompt-only requests without a string `prompt.id` before sending. Explicit-model routing is unchanged.
