Fixed

- Keep Kimi Platform model IDs and select its Anthropic Messages endpoint without changing Coding model normalization.
- Preserve Kimi temperature validation and use model-specific reasoning fields; unsupported Coding reasoning levels snap to the model's nearest level.
- Send Kimi grammar custom tools natively, preserving calls and results instead of converting them to functions.
- Forward Kimi request nonces and return the official proof headers for buffered and streamed responses by default.
