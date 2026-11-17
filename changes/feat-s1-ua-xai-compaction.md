Changed

- API-key upstream requests send `User-Agent: cooker`, except Claude requests shaped by cloaking or a confirmed Claude Code identity. Those headers and OAuth client identities stay unchanged.
- xAI compaction triggers go to xAI through ordinary generation routing. The gateway returns the upstream response instead of fabricating compaction events.
