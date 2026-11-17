Fixed

- An upstream response cut inside its last line now fails the attempt instead of completing it. Before, the Gemini, Vertex, Antigravity, OpenAI-compatible, xAI and Kimi executors treated the cut line as a complete final line. A Responses client could then receive a normal completion of a truncated answer.
- A Claude stream cut before the blank line that ends its last event now reports `unexpected EOF`, even when that event carries `message_stop`.
- A failed Antigravity attempt from a Claude client records the latest usage counts the upstream sent before the failure, zero counts and cache reads included. A local input-token estimate, a zero default or a count derived from the total no longer stands in for a missing count.
