Fixed

- Responses clients on Antigravity Claude models receive the upstream's thinking and tool-call signatures unchanged in the reasoning item's `encrypted_content`, whitespace included, and a returned signature reaches the upstream as the client holds it, so signed thinking and signed tool calls survive the next turn.
- Gemini and Responses clients on Antigravity Claude models send signed function calls back upstream with their signature; it was stripped before.
- Gemini, Chat and Responses clients on Antigravity Claude models no longer have their single-layer thinking signatures re-wrapped before the upstream; only the legacy `<group>#` prefix is removed. A signature a Gemini client sends in `extra_content.google.thought_signature` moves to the part's `thoughtSignature`.
- Antigravity reasoning (Claude signatures, CLIProxyAPI's Gemini carriers and compaction capsules) is no longer forwarded to Codex or any other Responses upstream; those destinations receive the request without that reasoning item.
