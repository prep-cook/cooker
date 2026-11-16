Fixed

- Usage records keep xAI's charge (`cost_in_usd_ticks` and `cost_in_nano_usd`), its source and server-side tool counts, and Codex's `codex_rollout_budget_units`, each as its own dimension in the unit the upstream reported. Rollout budget units keep the upstream's number as written, fractions included. A unit the upstream did not report is left out, not written as zero.
- xAI image and video responses record their usage. A video job is charged once, at the first poll that reports usage; later polls of the same job record no usage. A video usage that reports a charge but no token counts leaves the token counts out instead of writing zeros.
