Security

- Errors, upstream error summaries, request logs, access logs, proxy warnings, management quota-probe responses, Claude sign-in errors and TUI errors no longer show a password, query key or fragment embedded in a URL. A URL keeps its scheme, host, port and path, drops the userinfo and prints every query value and the fragment as `***`; a URL that cannot be split safely prints as `<redacted url>`.
- Request logs, their temporary spool files and server logs redact every URL as it is written, including URLs in any header, in captured bodies and split across writes. A relative reference carrying a query or fragment (`next#…`, `#…`) prints as `<redacted url>`. Clients and upstreams still receive the original bytes.
