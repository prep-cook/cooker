Changed

- Kimi credentials run Chat Completions and native Responses generation on the execution contract. Chat, Responses and Gemini clients send the same upstream request as before: Kimi's model, thinking, message, tool and temperature normalizations, then the payload rules last. A Gemini client's text now reaches Kimi as a content part, and a Gemini client always streams from Kimi.
- Claude Messages clients, every Kimi token count, and Responses requests that declare, choose or replay Codex's freeform `apply_patch` tool keep the previous route.
- A Kimi compact request is refused as unsupported at the request fence.
- A Kimi stream that ends without a finish (Chat) or without `response.completed` or `response.incomplete` (Responses) now fails as an upstream error instead of ending without one.
