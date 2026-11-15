Internal

- xAI credentials run on the execution contract: generation always streams `/responses` from the Responses request egress, xAI's response rules feed the Responses ingress, compact goes to `/responses/compact`, and token counts stay local. Reasoning replay is keyed by caller, credential and the exact session, and cached only after a completed response with no upstream error before it; replay entries are bounded to 8 MiB each and 256 MiB in total. A request carrying `compaction_trigger` keeps the old route.
