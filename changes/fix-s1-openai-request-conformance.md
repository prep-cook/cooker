Fixed

- OpenAI-compatible relays receive `max_completion_tokens` as the client sent it. It was renamed to the deprecated `max_tokens` unless the model set `use-max-completion-tokens`, which o-series models reject. A model with that setting still has `max_tokens` renamed to `max_completion_tokens`.
- Chat Completions requests are checked against the documented limits: an empty `messages` array, `temperature` outside 0–2, `top_p` outside 0–1, penalties outside -2–2, `top_logprobs` above 20, a `logit_bias` value outside -100–100, more than four `stop` sequences, or an `image` output modality now fail with 400.
- A Chat history tool call whose `arguments` string is not valid JSON reaches Chat and Responses upstreams unchanged instead of failing the request. Anthropic and Gemini upstreams, which need a JSON object, refuse it.
- A Chat upstream's `tool_calls` finish with arguments that are not valid JSON is delivered to the client instead of ending in an upstream error.
- Responses requests accept `instructions: null`, and compact requests accept `input: null`, as absent values.
