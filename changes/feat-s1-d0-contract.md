Internal

- The Phase D execution contract is fixed in code and in `docs/EXECUTION-CONTRACT.md`: stream ownership and quotas, the executor trait, request handle and per-attempt fence, response producer, server exchange and stream handle, cancel and discard, and the usage tracker input.
- Stream writers gain allocation handles, which let response egress and resume allocate on a stream whose producer runs or has finished, and holds, which keep a request stream cached while its response stream is.
- The Anthropic, Responses and Gemini boundary codecs build as production modules of `ck-translate`, and `ck-exec` and `ck-server` may depend on `ck-stream`.
- Codec flush deadlines read the stream store's clock, so tests advance a manual clock instead of sleeping, and the streamed audio test checks its exact record count again.
