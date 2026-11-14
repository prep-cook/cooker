Fixed

- Chat Completions clients get `role: "assistant"` in every non-streamed message and once, in the first chunk, of each streamed choice, whatever the upstream protocol.
- A Chat Completions tool call from a Responses upstream carries the upstream `call_id` instead of the output item id.
- Chat Completions streams no longer carry native events or SSE fields from an Anthropic, Responses or Gemini upstream.
