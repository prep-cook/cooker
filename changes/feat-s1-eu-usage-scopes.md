Changed

- The usage queue writes one record per usage scope: an attempt's own usage, and beside it one record per image tool model, upstream response or video job the upstream reported, marked by `usage_scope` (`tool:<model>`, `job:<id>`). Before, the last of these snapshots overwrote the others in a single record.
- A video job's usage is recorded once, at the first answer that reports it, whether the job is polled through the buffered video route or the executor; later polls of the same job record no usage.
- A video poll through the executor of a job this server did not create since it started is refused with 404 before anything is sent, as on the buffered route.
- xAI image and video usage served through the executor keeps `cost_in_usd_ticks`, which it dropped before.
