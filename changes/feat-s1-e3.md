Internal

- Add mapped codecs for bounded speech requests and audio-file responses, preserving content type and binary bytes while rejecting truncated downloads and oversized requests.
- Refuse unsupported xAI speech instructions instead of dropping them. Keep live audio, executors and routes outside this codec change.
- Retain native speech output codec, sample rate and bit rate in canonical audio parameters and xAI requests. Refuse `output_format` for OpenAI, a request with both `response_format` and `output_format`, unknown `output_format` members and zero rates.
- Publish canonical media data without frame alignment while retaining the 50,000-byte record ceiling.
