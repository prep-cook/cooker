Internal

- xAI credentials run on the execution contract: generation always streams `/responses` from the Responses request egress, xAI's response rules feed the Responses ingress, compact goes to `/responses/compact`, and token counts stay local. Reasoning replay is keyed by caller and credential and cached only after a completed response. A request carrying `compaction_trigger` keeps the old route.
