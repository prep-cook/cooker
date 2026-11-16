Fixed

- A Codex or Meta response that fails mid-stream with `response.failed` keeps the usage that event reports, so the failed request's usage record counts its tokens. The attempt keeps the provider's failure status and scope, and a response already sent to the client is not retried.
- Meta's in-stream `response.failed` takes its status from `response.error.code`, where Meta puts it, instead of always reporting 502.
- The Responses `error` event sent to clients carries `code`, `message` and `param` at its top level, as the OpenAI schema and SDKs expect, instead of inside a nested `error` object.
