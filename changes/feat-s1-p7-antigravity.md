Changed

- On Antigravity Claude models, clients receive thinking and tool-call signatures exactly as the upstream sent them, and the signatures they send back go upstream unchanged. A thinking block without a valid Claude signature is dropped; no cached signature is filled in.
- Antigravity credentials can run on the execution contract for Chat, Responses, Anthropic and Gemini clients. A Gemini `:generateContent` call stays unstreamed upstream, the `alt` query value reaches the upstream URL, and `/responses/compact` is answered from a local summary as before.
