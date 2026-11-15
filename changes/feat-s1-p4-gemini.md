Internal

- Gemini API keys run through the executor pipeline: requests are encoded by the Gemini request codec, streamed generations call `streamGenerateContent?alt=sse`, token counts call `countTokens`, and Anthropic clients counting against Gemini receive `input_tokens`.
