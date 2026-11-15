Fixed

- A Gemini response chunk with an empty `finishReason` no longer ends its candidate; later chunks for that candidate are delivered.
- A token count on a Vertex API key bound for Google's Vertex host is refused before sending, because Vertex accepts API keys only for generation. Service accounts and relay base URLs still count.
- A Vertex request for more than one candidate calls `generateContent`, because Vertex does not stream multiple candidates.
