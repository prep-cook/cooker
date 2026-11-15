Fixed

- AI Studio relay requests preserve native Gemini safety settings (including omission), output-token limits, response MIME types and schema dialects. Claude requests retain their output-token limits. Streamed Gemini responses retain nonterminal `usageMetadata` under its official name. Gemini and Vertex request behavior is unchanged.
