Internal

- Add the shared mmap stream store with complete-record publication, pinned reader leases, read-based retention and bounded storage. Production inference remains on its existing path until the S1 execution cutover.
- Add the production-versus-heap storage gate with raw samples, comparison distributions, memory accounting, pinned total-cap and isolated ENOSPC workloads. Failed or missing rows return a nonzero exit status.
- Keep read-retention ownership on each stream to avoid process-wide reference-count contention on every delivered record.
- Avoid redundant atomic read-modify-write during locked publication and notification registration for ready records, including immediately after a wakeup.
- Use an adaptive per-stream metadata mutex to reduce contention without changing publication or retention ordering.
- Reuse reserved frame coordinates during payload writes instead of recomputing byte-to-frame division for each span; keep checked frame lookup and copy bounds.
- Add independently releasable unpublished scratch leases and external metadata permits sharing canonical frame and metadata quotas. Scratch stays invisible to readers and pins borrowed bytes through invalidation; reader admission failures now publish the same storage-exhausted terminal as allocation failures.
- Add a startup-validated, deploy-only warm reserve within the pool cap. Preback it at pool open and preserve it during idle decay; reclaim excess frames under the existing interval and per-shard rules.
- Expose an owned producer cancellation future for terminal notifications; keep quiet attached readers live, bound stale-head eviction and remove unused mapped header counters. Cover independent reader cursors/pins, invalidation fanout, concurrent shutdown/eviction rollback and release without punching.
