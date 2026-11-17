Fixed

- Responses requests with `instructions` or `max_output_tokens`, and Chat requests with `max_completion_tokens` or a `developer` message, reach Anthropic upstreams instead of being refused. The instructions become the system prompt and the limit becomes `max_tokens`.
- Gemini requests whose `generationConfig` holds only mapped controls, such as `temperature`, `maxOutputTokens` and `stopSequences`, reach Chat and Anthropic upstreams instead of being refused.
- Anthropic requests with `tool_result` blocks reach Chat upstreams: each result becomes a `tool` message with its text, ahead of the rest of the turn. Chat has no error flag, so `is_error` is dropped and the text is kept. A result containing an image is still refused.
- Anthropic requests with a `thinking` config no longer send an unknown `reasoning` object to Chat upstreams. The effort still becomes `reasoning_effort`.
- A Chat `reasoning` object's `effort` now sets Gemini's `thinkingLevel`, as `reasoning_effort` does, instead of sending an empty `thinkingConfig`.
- Gemini requests with `thinkingConfig.thinkingBudget` reach Anthropic and Chat upstreams. A positive budget enables thinking with that budget, 0 disables it, and -1 (dynamic) becomes adaptive thinking on Anthropic.
- Gemini requests with `toolConfig.functionCallingConfig` reach Chat and Anthropic upstreams. Modes AUTO, ANY and NONE become `auto`, `required` and `none`; ANY with `allowedFunctionNames` becomes Chat's `allowed_tools`, and on Anthropic a single name forces that tool. More than one name is refused on Anthropic.
- Chat `allowed_tools` choices in `required` mode reach Gemini as mode ANY with `allowedFunctionNames`, and reach Anthropic when they list one tool.
- Gemini function schemas reach Chat and Anthropic upstreams as valid JSON Schema: types are lowercase, `nullable` becomes a `"null"` type, and `propertyOrdering` is left out. A schema JSON Schema cannot express, such as `TYPE_UNSPECIFIED`, is refused.
- A request that forces a tool while asking for budgeted thinking is refused before it reaches Anthropic, which does not allow the combination. Adaptive thinking with a forced tool is still sent.
- Gemini function declarations with `parametersJsonSchema` reach Chat and Anthropic upstreams with that schema as the tool's parameters. `response`, `responseJsonSchema` and the default `behavior` are not sent there. A declaration with both schema forms, or with `behavior: NON_BLOCKING`, is refused.
