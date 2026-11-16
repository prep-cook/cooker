Fixed

- Responses clients on Antigravity Claude models receive the upstream's thinking and tool-call signatures unchanged in the reasoning item's `encrypted_content`, whitespace included, and a returned thinking signature reaches the upstream byte for byte, so signed thinking survives the next turn.
- A Responses conversation continued with `previous_response_id` on the same Antigravity account sends its signed function calls back upstream with their signatures. A signature a client replays itself is still withheld, since nothing proves which provider minted it.
- Gemini, Chat and Responses clients on Antigravity Claude models no longer have their single-layer thinking signatures re-wrapped before the upstream; only the legacy `<group>#` prefix is removed, and surrounding whitespace is kept. A signature a Gemini client sends in `extra_content.google.thought_signature` moves to the part's `thoughtSignature`.
- Claude-format reasoning and CLIProxyAPI's Antigravity carriers and compaction capsules that a client replays itself are no longer forwarded to Codex or any other Responses upstream; those destinations receive the request without that reasoning item.
