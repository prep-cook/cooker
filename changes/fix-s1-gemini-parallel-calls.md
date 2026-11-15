Fixed

- A Responses client's parallel tool calls reach Gemini as one model turn, followed by one user turn holding their results in the order of the calls, instead of being refused as unsupported.
- Tool results from an Anthropic client reach Gemini in the order of their calls, so Gemini models that pair results with calls by position attach each result to its own call.
