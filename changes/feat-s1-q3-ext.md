Changed

- A request carrying Anthropic `cache_control` breakpoints is no longer refused on its way to a Responses or Gemini upstream; the breakpoints reach Anthropic upstreams only.
- A Chat request's `cache_control` reaches an Anthropic upstream on the block it marked, and Chat upstreams no longer receive it.
- Anthropic upstreams no longer receive fields a Gemini or Responses client sent that have no Anthropic meaning, such as Gemini `partMetadata` or the Responses item `type`.
