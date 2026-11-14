Fixed

- Gemini requests with images from the google-genai SDK, which encodes inline data in the URL-safe base64 alphabet, are accepted for every upstream.
- Gemini requests with tools from the google-genai SDK, which always sends an empty `generationConfig`, reach Chat and Anthropic upstreams.
- A Gemini function response bound for an Anthropic upstream is refused as unsupported instead of being sent without its result.
- Gemini requests that spell fields in snake_case, such as `thinking_budget` or `parameters_json_schema`, translate like their camelCase spelling.
- A Gemini response schema bound for a Chat or Anthropic upstream, or a Responses reasoning summary bound for a Chat upstream, is refused as unsupported rather than as a malformed request.
- Requests sent to a Responses upstream carry only `effort` and `summary` in `reasoning`. Thinking enabled without an effort, such as an Anthropic token budget, is refused as unsupported instead of reaching the upstream as invalid members.
- Chat requests whose assistant history has `tool_calls: null` or `function_call: null`, as the openai SDK writes them, are accepted.
- Thinking blocks with an empty signature are left out of requests to an Anthropic upstream, which refuses them.
