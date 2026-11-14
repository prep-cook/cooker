Internal

- The Phase D execution contract is fixed in code and in `docs/EXECUTION-CONTRACT.md`: the executor trait, request handle, response producer, server exchange and stream handle, and the usage tracker input.
- The Anthropic, Responses and Gemini boundary codecs build as production modules of `ck-translate`, and `ck-exec` and `ck-server` may depend on `ck-stream`.
- Codec flush deadlines read the stream store's clock, so tests advance a manual clock instead of sleeping, and the streamed audio test checks its exact record count again.
