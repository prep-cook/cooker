Internal

- Vertex runs through the executor pipeline: Google models are encoded by the Gemini request codec and call the publisher model's `streamGenerateContent?alt=sse`, `generateContent` or `countTokens`; partner and open models take `endpoints/openapi` through the OpenAI-compatible relay's Chat path with a local count. Imagen models keep the old path.
