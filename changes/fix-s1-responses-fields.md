Fixed

- A Responses request's `top_logprobs` reaches a Responses upstream as an integer; Gemini and Anthropic upstreams refuse it as unsupported instead of dropping it or sending it unchanged.
- A Chat request's nested `allowed_tools` tool choice reaches a Responses upstream in the flat Responses shape; a member Responses has no place for is refused as unsupported.
- A Chat request's `logprobs: true` is refused as unsupported by a Responses upstream, which has no such member; `logprobs: false` or null is no longer sent to it.
- A Chat upstream refuses `top_logprobs` as unsupported unless the request also sets `logprobs: true`, which the Chat API requires; a Responses request's `top_logprobs` is therefore refused for a Chat upstream.
