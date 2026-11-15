Internal

- OpenAI-compatible relays run on the execution contract: generation goes out as Chat through the request codecs, compact through the relay's native `/responses/compact`, and token counts stay local.
- A Responses client's response repeats its `tools`, `tool_choice` and `parallel_tool_calls` when the upstream does not report them.
