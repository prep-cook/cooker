Security

- Request logs redact URL credentials in each captured section as a whole, so a URL split across upstream response chunks no longer prints its userinfo, query values or fragment.
- In a WebSocket timeline, a URL or credential-like word that one message ends with and the next may continue prints as `<redacted url>`, and the next message is dropped up to its first whitespace or `"`.
- When a capture spool cannot be written, the request log shows `[capture dropped: write failed]` for that section instead of a partial copy.
- Capture spool files hold the captured bytes unredacted until the log is written, and no longer appear in the log directory: each is unlinked as soon as it is created.
- A URL inside another URL's path (`/r/https://user:key@host/`) is redacted on its own; a `scheme://` or `//` inside an authority (`https://user:key://host`) prints as `<redacted url>`.
- A word that could still carry a credential after redaction (a `//` not followed by a host and port, or `scheme:key@host`) prints as `<redacted url>`.
- A request log is redacted as it is written, never read into memory whole; a word or unfinished URL longer than 1 MiB prints as `<redacted url>`.
- Captured upstream response bodies join chunks exactly as received; line-by-line streams keep each line's newline.
