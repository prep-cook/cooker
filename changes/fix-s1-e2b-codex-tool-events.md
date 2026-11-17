Fixed

- Preserve Codex image tool events' SSE source fields and unknown event and image members in the canonical stream, including previews and delayed results.
- Store completion SSE fields once across Codex images, and defer encoding delayed calls' unknown members until publication.
- Preserve streamed OpenAI image extras at their native event-root pointers.
