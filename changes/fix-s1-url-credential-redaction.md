Security

- Errors, upstream error summaries, request logs, access logs, proxy warnings, management quota-probe responses, Claude sign-in errors and TUI errors no longer show a password, query key or fragment embedded in a URL. A URL a caller names on its own keeps its scheme, host, port and path, drops the userinfo and prints every query value and the fragment as `***`; a URL that cannot be split safely prints as `<redacted url>`. A URL inside free text prints as `<redacted url>`.
- Request logs and server logs redact every URL, including URLs in any header and in captured bodies. A relative reference carrying a query or fragment value (`callback?code=…`, `next#token=…`) prints as `<redacted url>`. Clients and upstreams still receive the original bytes.
