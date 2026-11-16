Security

- Request logs, the server log and error text are redacted word by word, a word being the bytes between whitespace and `"`. A word that holds `://`, an `@`, a `?` or `#` with an `=` after it, or a letter and `:` with `//` after them prints as `<redacted url>` whole, so every URL in a log prints as the placeholder. E-mail addresses, `user@host`, `mailto:` addresses and words such as `?a=b` print as the placeholder too.
- After such a word, a quoted value with spaces in it is dropped up to its closing `"`, or up to the line end for a value opened by `'`, a backtick or `<`.
- A fragment or query with no `=` (`next#token`, `callback?token`) is no longer redacted.
- A word longer than 1 MiB prints as `<redacted url>`, in memory and in a spooled section alike.
- Request logs redact each captured section as a whole, so a URL split across upstream response chunks no longer prints its credentials. A URL that a protocol splits across separate JSON values (stream deltas) is still redacted value by value.
- In a WebSocket timeline, a message that ends in a word the next message may continue prints that word as `<redacted url>`, and the next message is dropped up to its first whitespace or `"`.
- When a capture spool cannot be written, the request log shows `[capture dropped: write failed]` for that section instead of a partial copy.
- Capture spool files hold the captured bytes unredacted until the log is written, and no longer appear in the log directory: each is unlinked as soon as it is created.
- A URL named on its own prints as `<redacted url>` when a `//` other than its opening one comes before its query or fragment (`/r/https://user:key@host/`, `https://user:key://host`).
- Captured upstream response bodies join chunks exactly as received; line-by-line streams keep each line's newline.
