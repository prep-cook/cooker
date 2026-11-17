Fixed

- A streamed Responses request served by xAI or Meta ends with the provider's terminal event followed by one `data: [DONE]`, as their APIs document, however the upstream split its final bytes. OpenAI and Kimi Responses streams and failed streams still end without it.
