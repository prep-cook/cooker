Fixed

- Anthropic tool results reach a Responses upstream as `function_call_output` items; a result marked `is_error: true` is refused as unsupported.
- Gemini function responses reach Chat, Anthropic and Responses upstreams as tool text: a string `result` as it is, any other `result` as compact JSON; an `error` reaches Anthropic as an error result and is refused by Chat and Responses.
- Gemini JSON mode reaches Chat and Responses as a `json_schema` or `json_object` response format, and a Gemini response schema reaches Anthropic as its JSON-schema output format.
- Gemini function declarations reach a Responses upstream as function tools with JSON Schema `parameters`.
- A Gemini model turn holding only function calls reaches a Chat upstream with empty text content instead of an empty content array.
- Gemini `includeThoughts` without a thinking budget reaches Responses as `reasoning.summary: "auto"`.
- A Gemini upstream takes a `developer` message or Responses `instructions` as its system instruction, `parallel_tool_calls: true`, `store: false`, and Anthropic `top_k` as `topK`; Claude thinking history is stripped as it is for Chat.
- An Anthropic upstream takes `store: false`.
- Anthropic `top_k` and other parameters of another protocol sent to a Responses upstream are refused as unsupported instead of being dropped.
