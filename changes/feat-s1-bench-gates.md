Internal

- Add S1 codec comparison, flow semantics and rootless container startup gates with retained raw observations, explicit harness contracts and negative controls. Missing measurements and unsupported server features fail instead of being skipped; codec and pipeline metrics allow at most 5% regression, while memory and storage allow none.
- Cover long realtime WebSocket control, media, sideband and reconnect flows, and enforce resume identity/cursor validation. Reject codec self-comparisons, retain overridable build settings in provenance, report leftover accounting groups, and require the configured small pool capacity.
