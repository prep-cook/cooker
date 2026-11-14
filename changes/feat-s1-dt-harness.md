Internal

- Executor and server tests share a local upstream that counts requests, scripts error statuses, resets, cuts, byte-at-a-time and throttled bodies and slow request reads, and reports when each body was fully written.
- Executor tests get helpers for a private stream store on a manual clock, the request fence, attempts and request handles.
