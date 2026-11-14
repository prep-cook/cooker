Fixed

- Anthropic `messages.stream()` clients no longer fail on a response from an OpenAI or buffered upstream: `message_start` always carries `usage.input_tokens` and `usage.output_tokens`, written as 0 until the final `message_delta` reports the real counts.
- A thinking block from an OpenAI-format upstream now carries `"signature": ""`, as the Anthropic API sends for an unsigned block.
- Anthropic clients no longer receive an empty text block, such as the one an OpenAI `content: ""` beside tool calls produced.
- A tool call from a Responses upstream reaches Anthropic clients with its `call_id`, not its `fc_` item id.
