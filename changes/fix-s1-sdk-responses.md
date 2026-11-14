Fixed

- Responses clients no longer receive members or events under cooker's reserved `x_ck` prefix, such as `x_ck_opaque`, which revealed the upstream protocol. An unknown event from a Responses upstream reaches a streaming Responses client as that event, numbered in sequence; unknown data from other protocols is dropped.
- Responses objects from Anthropic, Chat and Gemini upstreams now carry `parallel_tool_calls`, `tool_choice` and `tools` at their API defaults (`true`, `"auto"`, `[]`), and `usage.input_tokens_details` always carries `cached_tokens` and `cache_write_tokens`, written as 0 when the upstream did not report them. A Responses upstream's own values still win.
- Streamed `response.output_text.delta` and `response.output_text.done` events always carry `logprobs`, an empty array when the upstream reported none.
