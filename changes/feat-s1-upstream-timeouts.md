Added

- Every upstream attempt on the Chat, Anthropic, Gemini, Responses and media routes has three deadlines: 600 s for response headers, 300 s between response body chunks and 3600 s in total. They are set under `requests.upstream-timeouts`. When a deadline expires before the client receives any output, the request moves to the next account. After output has started, the client gets an error event and the request is not retried.
