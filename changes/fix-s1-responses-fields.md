Fixed

- A Responses request's `top_logprobs` reaches Chat and Responses upstreams as an integer; Gemini and Anthropic upstreams refuse it as unsupported instead of dropping it or sending it unchanged.
- A Chat request's nested `allowed_tools` tool choice reaches a Responses upstream in the flat Responses shape; a member Responses has no place for is refused as unsupported.
