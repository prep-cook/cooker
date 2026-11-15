Added

- A client that loses a committed response reconnects with `GET /v1/streams?after=<cursor>` and the `X-Ck-Stream-Id` header, and receives exactly the part it missed, live or finished, without a new upstream request. SSE events carry `id: v1.e.<n>` cursors; JSON bodies resume by byte (`v1.b.<n>`). An unknown, expired, evicted or foreign stream answers 410 `stream.gone`.
- Request and error logs redact the `X-Ck-Stream-Id` and `Last-Event-ID` headers.
- Usage records omit token counts the upstream did not report instead of recording zero.
