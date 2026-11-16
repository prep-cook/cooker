Changed

- A `responses/compact` request now moves on to the next credential when an upstream answers 404, 405 or 501, and the credential is tried again by the next compact instead of being cooled.
- Compact failures that are transport faults, 408 or 5xx fail over and count against the credential like other requests; a 400, 409, 413 or 422 stops the request without trying another credential.
- A compact is no longer sent to providers that have no native compact (Claude, Antigravity and the other providers outside Codex, xAI and OpenAI-compatible relays); the client gets the compaction refusal.
