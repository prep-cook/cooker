Security

- Errors, upstream error summaries, request logs, access logs, proxy warnings, management quota-probe responses, Claude sign-in errors and TUI errors no longer show a password, query key or fragment embedded in a URL. A URL keeps its scheme, host, port and path, drops the userinfo and prints every query value and the fragment as `***`; a URL that cannot be split safely prints as `<redacted url>`.
- Request logs and server logs redact every URL they write, including URLs in any response header and in captured upstream error bodies. Clients and upstreams still receive the original bytes.
