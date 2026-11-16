Changed

- The usage queue writes one record per usage scope: an attempt's own usage, and beside it one record per image tool model or video job the upstream reported, marked by `usage_scope` (`tool:<model>`, `job:<id>`). Before, the last of these snapshots overwrote the others in a single record.
- A video job's usage is recorded once, at the first poll that reports it; later polls of the same job record no usage.
- xAI image and video usage served through the executor keeps `cost_in_usd_ticks`, which it dropped before.
