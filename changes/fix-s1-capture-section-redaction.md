Security

- Request logs, the server log and error text are redacted word by word, a word being the bytes between whitespace and `"`. A word that holds `://`, an `@`, a `?` or a `#`, or a scheme (a letter, then letters, digits, `+`, `-` or `.`) followed by `:` and at least one more byte prints as `<redacted url>` whole, so every URL in a log prints as the placeholder. E-mail addresses, `user@host`, `mailto:` addresses, opaque URLs (`https:key`), `name:port` (`example.com:443`), SDP attribute lines and words such as `what?` or `C#` print as the placeholder too. Times and IPv4 `address:port` print, and the server log's own time and source-location prefix is not redacted.
- A character after a backslash belongs to the word, so an escaped quote (`\"`) in a JSON string or a quoted error no longer ends a redacted word early.
- After such a word, a quoted value with spaces in it is dropped up to its closing unescaped `"`, also when the word sits anywhere inside a `"` string, or up to the line end for a value opened by `'`, a backtick, `<` or an escaped `"`.
- A word longer than 1 MiB prints as `<redacted url>`, in memory and in a spooled section alike.
- Request logs redact each captured section as a whole, so a URL split across upstream response chunks no longer prints its credentials. A URL that a protocol splits across separate JSON values (stream deltas) is still redacted value by value.
- In a WebSocket timeline, the word a message ends in prints as `<redacted url>` whenever another message follows, whatever its last character, and the next message is dropped up to the end of its first word.
- When a capture spool cannot be written, the request log shows `[capture dropped: write failed]` for that section instead of a partial copy.
- Capture spool files hold the captured bytes unredacted until the log is written, and no longer appear in the log directory: each is unlinked as soon as it is created.
- A URL named on its own prints as `<redacted url>` when a `//` other than its opening one comes before its query or fragment (`/r/https://user:key@host/`, `https://user:key://host`).
- Captured upstream response bodies join chunks exactly as received; line-by-line streams keep each line's newline.
