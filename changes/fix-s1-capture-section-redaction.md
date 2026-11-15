Security

- Request logs redact URL credentials in each captured section as a whole, so a URL split across upstream response chunks no longer prints part of its credential.
- When a capture spool cannot be written, the request log shows `[capture dropped: write failed]` for that section instead of a partial replay.
- Capture spool files no longer appear in the log directory; they are removed from it as soon as they are created.
- A URL inside another URL's path (`/r/https://user:key@host/`) is redacted on its own instead of printing its credentials as part of the path.

Changed

- Captured upstream response bodies join chunks exactly as received, without blank lines between them or trimmed whitespace; line-by-line streams keep each line's newline.
- A spooled capture is redacted as it is copied into the request log; a URL still unfinished after 1 MiB prints as `<redacted url>`.
