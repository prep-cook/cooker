Fixed

- Preserve xAI safety identifiers, explicit parallel-tool settings, search parameters and up to 1,000 tools; refuse over-limit requests and generation controls the Responses transport cannot honor before sending.
- Keep native xAI reasoning events, Responses X Search traces and Chat usage extensions. Anthropic, Chat and Gemini projections drop hosted search traces while retaining client-declared tools with the same names; Gemini streaming retains both reasoning-text event variants.
