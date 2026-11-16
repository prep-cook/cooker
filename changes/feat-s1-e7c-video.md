Changed

- `POST /v1/alpha/search` and `/backend-api/codex/alpha/search` fail over to the next eligible Codex credential when the upstream refuses a search, instead of returning the upstream's status and body. The client gets the search result JSON as `application/json`, with the members the upstream sent; the response carries `X-Ck-Stream-Id` and resumes with `GET /v1/streams?after=v1.b.<bytes>`.
- A search body that is not a JSON object with a string `query`, or that repeats a member, is refused with 400 before anything is sent; a body over 16 MiB is refused with 413 instead of being cut. Errors use the OpenAI error shape (`{"error":{"message":…,"type":…}}`) instead of `{"error":"…"}`.
- A Codex API key with `alpha-search: true` but no base URL is no longer selected for Alpha Search.
