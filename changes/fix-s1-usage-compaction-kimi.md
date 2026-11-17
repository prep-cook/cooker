Fixed

- Usage records of an Anthropic compaction count the tokens and cache-write TTL buckets reported in `usage.iterations` instead of the zero top-level counts.
- Usage records of a streamed Kimi Chat response that reports usage per choice count the prompt once and every choice's completion; an aggregate usage replaces them, and a cut stream keeps the usage already reported.
- Usage records of Kimi Chat and Responses cache writes fill the 5-minute or 1-hour TTL bucket the write was billed at, taken from the Chat request's `prompt_cache_options.ttl` or the TTL a Responses response says it applied.
