Security

- Errors, request logs, access logs and management quota-probe responses no longer show a password, query key or fragment embedded in a URL. A URL keeps its scheme, host, port and path, drops the userinfo and prints every query value and the fragment as `***`; a URL that cannot be split safely prints as `<redacted url>`.
- Request logs redact URLs in `Location`, `Content-Location`, `Referer`, `Link` and similar headers. Clients still receive those headers unchanged.
