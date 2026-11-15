Security

- Request logs redact URL credentials in each captured section as a whole, so a URL split across upstream response chunks no longer prints part of its credential.
- When a capture spool cannot be written, the request log shows `[capture dropped: write failed]` for that section instead of a partial replay.
- Capture spool files no longer appear in the log directory; they are removed from it as soon as they are created.

Changed

- Captured upstream response bodies join chunks exactly as received, without blank lines between them or trimmed whitespace.
