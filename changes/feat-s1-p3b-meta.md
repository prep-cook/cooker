Changed

- Meta credentials run on the execution contract: every client protocol is encoded by the Responses request codec and shaped with Meta's rules, generation always streams from `/responses`, and token counts stay local. A request with Codex's freeform `apply_patch` tool keeps the previous route.
- A Meta compact request is refused as unsupported at the request fence.
- A Meta stream with invalid UTF-8 fails as an upstream error instead of passing repaired text, and a Meta stream that ends before `response.completed` or `response.incomplete` fails with Meta's disconnect error (408) for streaming clients too.
