Fixed

- Meta requests preserve explicit token budgets, sampling, output includes, parallel-tool controls, service tiers, cache hints and end-user identifiers instead of applying Codex policy. Unsupported Chat controls and automatic truncation fail rather than being silently discarded. Omitted reasoning effort stays omitted; the Meta `apply_patch` bridge remains enabled.
