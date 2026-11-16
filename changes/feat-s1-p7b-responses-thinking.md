Fixed

- Responses clients on Antigravity Claude models receive the upstream's thinking and tool-call signatures unchanged in the reasoning item's `encrypted_content`, and a returned thinking signature reaches the upstream as the client holds it, so signed thinking survives the next turn.
- Gemini, Chat and Responses clients on Antigravity Claude models no longer have their single-layer thinking signatures re-wrapped before the upstream; only the legacy `<group>#` prefix is removed.
