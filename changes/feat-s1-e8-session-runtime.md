Internal

- Live sessions (Realtime, Live, WebRTC sideband) run on one runtime-owned task. It checks each client message before it reaches the upstream, reports each response with its origin, notices a client or upstream disconnect even while a write is stalled, and frees its storage when it ends. Until then other exchanges cannot evict its records and they do not expire.
- A live session holds a pool frame for its final counters from the start, so it publishes them at every end, including shutdown, a full stream quota and a pool other exchanges have filled, and reports when they could not be stored.
