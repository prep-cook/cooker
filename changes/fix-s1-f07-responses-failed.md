Fixed

- A Codex or Meta response that fails mid-stream with `response.failed` keeps the usage that event reports, so the failed request's usage record counts its tokens. The attempt keeps the provider's failure status and scope, and a response already sent to the client is not retried.
- A failed Responses stream's final response shows the partial output and item status from the upstream's failed snapshot, not only the text streamed before it.
- Meta's in-stream `response.failed` takes its status, subscription-quota scope and reset time from `response.error`, where Meta puts them. Before, it always reported 502; a nested quota exhaustion now cools the whole credential until its reset time instead of only the model.
- The Responses `error` event sent to clients carries `code`, `message` and `param` at its top level, as the OpenAI schema and SDKs expect, instead of inside a nested `error` object.
